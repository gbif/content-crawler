/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.content.crawl.contentful.crawl;

import com.google.common.collect.Sets;
import org.gbif.content.crawl.es.ElasticSearchUtils;

import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.LocalizedResource;
import com.contentful.java.cma.Constants;
import com.google.common.collect.Lists;

/**
 * Translates a CDAEntry into Map object indexable in ElasticSearch.
 */
public class EsDocBuilder {

  private static final String REGION_FIELD = "gbifRegion";

  private static final Pattern LINKED_ENTRY_FIELDS = Pattern.compile(".*summary.*|.*title.*|.*body.*|label|url|country|isoCode");

  private static final List<String> LOCALIZED_FIELDS = Lists.newArrayList("title","body","summary","description");

  private static final String LOCALE_FIELD = "locale";
  private static final String ID_FIELD = "id";

  private static final String CONTENT_TYPE_FIELD = "contentType";

  private final String projectContentTypeId;

  private final Map<String,Object> entries;
  private final ContentTypeFields contentTypeFields;
  private final VocabularyTerms vocabularyTerms;
  private final Consumer<Object> nestedEntriesConsumer;
  private final CDAEntry cdaEntry;

  /**
   * Creates EsDocBuilder instance.
   * @param cdaEntry Contentful resource to be translated
   * @param vocabularyTerms Vocabulary metadata
   * @param nestedEntriesConsumer Consumer of nested entries
   */
  public EsDocBuilder(CDAEntry cdaEntry, VocabularyTerms vocabularyTerms, String projectContentTypeId, Consumer<Object> nestedEntriesConsumer) {
    this.vocabularyTerms = vocabularyTerms;
    this.nestedEntriesConsumer = nestedEntriesConsumer;
    this.cdaEntry = cdaEntry;
    contentTypeFields = ContentTypeFields.of(cdaEntry.contentType());
    entries = new HashMap<>();
    this.projectContentTypeId = projectContentTypeId;
  }

  private Optional<String> getProgrammeAcronym(CDAEntry cdaEntry) {
    if (projectContentTypeId.equals(cdaEntry.contentType().id()) &&  cdaEntry.rawFields().containsKey("programme")) {
      CDAEntry programme = cdaEntry.getField("programme");
      if (programme != null && programme.rawFields().containsKey("acronym")) {
        return Optional.of(programme.getField("acronym"));
      }
    }
    return Optional.empty();
  }

  /**
   * Gets the ElasticSearch document of a CDAEntry.
   */
  public Map<String,Object> toEsDoc() {
    cdaEntry.rawFields().forEach((field,value) -> {
      Optional.ofNullable(cdaEntry.getField(field)).ifPresent(fieldValue -> {
        Constants.CMAFieldType fieldType = contentTypeFields.getFieldType(field);
        if (Constants.CMAFieldType.Link == fieldType) {
          processLinkField((LocalizedResource)fieldValue, field);
        } else if (Constants.CMAFieldType.Array == fieldType) { //block are processed differently
          processArrayField((List<?>)fieldValue, field);
        } else {
          entries.put(field, contentTypeFields.getField(field).isLocalized()
                             && !contentTypeFields.isCollapsible(field) ? value : fieldValue);
        }
      });
    });
    entries.putAll(cdaEntry.attrs());
    //Updates the information from the meta field
    Meta.getMetaCreatedDate(cdaEntry).ifPresent(createdDate -> entries.replace("createdAt", createdDate));
    getBlocks(cdaEntry).ifPresent(blocks -> entries.put("blocks", blocks));
    entries.put(CONTENT_TYPE_FIELD, ElasticSearchUtils.toFieldNameFormat(cdaEntry.contentType().name()));
    getProgrammeAcronym(cdaEntry)
      .ifPresent(programmeAcronym -> entries.put("gbifProgrammeAcronym", programmeAcronym));
    entries.put(LOCALE_FIELD, getLocales(cdaEntry));
    return entries;
  }

  private Optional<Map<String,Object>> getBlocks(CDAEntry cdaEntry) {
    if (cdaEntry.getField("blocks") != null) {
      Map<String,Object> blockFields = new HashMap<>();
      List<CDAEntry> blocks = cdaEntry.getField("blocks");
      blocks.forEach(block ->  {
        String blockName = block.contentType().id();
        Object value = blockFields.get(blockName);
        if (value == null) {
          value = Lists.newArrayList(block.rawFields());
        } else {
          ((ArrayList<Map<String, Object>>)value).add(block.rawFields());
        }
        blockFields.put(blockName, value);
      });
      return Optional.of(blockFields);
    }
    return Optional.empty();
  }

  /**
   * Collects fields data from a single localized resource.
   * @param resource Asset or CDAEntry to be processed
   * @param field field name
   */
  public void processLinkField(LocalizedResource resource, String field) {
    if (ContentfulLinkType.Asset == contentTypeFields.getFieldLinkType(field)) {
      entries.put(field,  resource.rawFields());
    } else {
      CDAEntry fieldCdaEntry = (CDAEntry)resource;
      VocabularyBuilder vocabularyBuilder = new VocabularyBuilder(vocabularyTerms);
      vocabularyBuilder.of(fieldCdaEntry)
        .one(vocValue -> entries.put(field, vocValue))
        .gbifRegion(gbifRegion -> entries.put(REGION_FIELD, gbifRegion));
      if (vocabularyBuilder.isEmpty()) {
        nestedEntriesConsumer.accept(fieldCdaEntry);
        entries.put(field, getAssociatedEntryFields(fieldCdaEntry));
      }
    }
  }

  /**
   * Processes an array field value.
   * @param entryListValue list of entries to be processed
   * @param field field name
   */
  private void processArrayField(List<?> entryListValue, String field) {
    if(!field.equalsIgnoreCase("blocks")) { //blocks are processed later
      VocabularyBuilder vocabularyBuilder = new VocabularyBuilder(vocabularyTerms);
      vocabularyBuilder.ofList(entryListValue)
              .all(vocValues -> entries.put(field, vocValues))
              .allGbifRegions(gbifRegions -> entries.put(REGION_FIELD, gbifRegions));
      if (vocabularyBuilder.isEmpty()) {
        nestedEntriesConsumer.accept(entryListValue);
        entries.put(field, toListValues(entryListValue));
      }
    }
  }

  /**
   * Extract the values as maps of the list of resources.
   */
  private List<?> toListValues(List<?> resources) {
    return resources.stream()
      .flatMap(resource -> {
        if (CDAEntry.class.isInstance(resource)) {
          return Stream.of(getAssociatedEntryFields((CDAEntry) resource));
        }
        if (LocalizedResource.class.isInstance(resource)) {
          return Stream.of(((LocalizedResource)resource).rawFields());
        }
        if (String.class.isInstance(resource)) {
          return Stream.of((String)resource);
        }
        return Stream.empty();
      })
      .collect(Collectors.toList());

  }

  /**
   * Associated entities are indexed using title, summary and id.
   */
  private Map<String, Object> getAssociatedEntryFields(CDAEntry cdaEntry) {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put(ID_FIELD, cdaEntry.id());
    fields.putAll(cdaEntry.rawFields().entrySet().stream()
                    //Content types Header Block, Feature Block do not have to be handled as Links
                    .filter(entry ->  (!cdaEntry.contentType().id().endsWith("Block") && LINKED_ENTRY_FIELDS.matcher(entry.getKey()).matches())  &&
                                      cdaEntry.getField(entry.getKey()) != null)  //a project had a null url value
                    .collect(Collectors.toMap(Map.Entry::getKey,
                                              entry -> getValue(entry, cdaEntry))));
    return fields;
  }

  private Object getValue(Map.Entry<String,Object> entry, CDAEntry cdaEntry) {
      Object value = cdaEntry.getField(entry.getKey());
      if (value instanceof CDAEntry) {
        //if the nested value is country only the isoCode is extracted
        Optional<String> countryField = vocabularyTerms.countryCodeFieldOf((CDAEntry)value);
        if (countryField.isPresent()) {
            return ((CDAEntry)value).getField(countryField.get());
        }
        return getAssociatedEntryFields((CDAEntry)value);
      }
      return isLocalized(entry.getKey(), cdaEntry.contentType())
              ? entry.getValue()
              : cdaEntry.getField(entry.getKey());
  }


  Set<String> getLocales(CDAEntry cdaEntry) {
    return LOCALIZED_FIELDS.stream()
              .filter(field -> cdaEntry.getField(field) != null && isLocalized(field, cdaEntry.contentType()))
              .flatMap(field -> ((Map<String,Object>)cdaEntry.rawFields().get(field)).keySet().stream())
              .collect(Collectors.toSet());
  }

  /**
   * Is the fieldName localized in the CDAContentType.
   */
  private static boolean isLocalized(String fieldName, CDAContentType cdaContentType) {
    return ContentTypeFields.of(cdaContentType).getField(fieldName).isLocalized();
  }

}
