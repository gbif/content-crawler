package org.gbif.content.crawl.contentful.crawl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.LocalizedResource;
import com.contentful.java.cma.Constants;

/**
 * Translates a CDAEntry into Map object indexable in ElasticSearch.
 */
public class EsDocBuilder {

  private static final String REGION_FIELD = "gbifRegion";

  private static final Pattern LINKED_ENTRY_FIELDS = Pattern.compile(".*summary.*|.*title.*|label|url");

  private static final String ID_FIELD = "id";

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
  public EsDocBuilder(CDAEntry cdaEntry, VocabularyTerms vocabularyTerms, Consumer<Object> nestedEntriesConsumer) {
    this.vocabularyTerms = vocabularyTerms;
    this.nestedEntriesConsumer = nestedEntriesConsumer;
    this.cdaEntry = cdaEntry;
    contentTypeFields = ContentTypeFields.of(cdaEntry.contentType());
    entries = new HashMap<>();
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
        } else if (Constants.CMAFieldType.Array == fieldType) {
          processArrayField((List<LocalizedResource>)fieldValue, field);
        } else {
          entries.put(field, contentTypeFields.getField(field).isLocalized()
                             && !contentTypeFields.isCollapsible(field) ? value : fieldValue);
        }
      });
    });
    entries.putAll(cdaEntry.attrs());
    //Updates the information from the meta field
    Meta.getMetaCreatedDate(cdaEntry).ifPresent(createdDate -> entries.replace("createdAt", createdDate));
    return  entries;
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
      if(vocabularyBuilder.isEmpty()) {
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
  private void processArrayField(List<LocalizedResource> entryListValue, String field) {
    VocabularyBuilder vocabularyBuilder = new VocabularyBuilder(vocabularyTerms);
    vocabularyBuilder.ofList(entryListValue)
      .all(vocValues -> entries.put(field, vocValues))
      .allGbifRegions(gbifRegions -> entries.put(REGION_FIELD, gbifRegions));
    if (vocabularyBuilder.isEmpty()) {
      nestedEntriesConsumer.accept(entryListValue);
      entries.put(field, toListValues(entryListValue));
    }
  }

  /**
   * Extract the values as maps of the list of resources.
   */
  private static List<Map<String,Object>> toListValues(List<LocalizedResource> resources) {
    return resources.stream()
      .map(resource -> CDAEntry.class.isInstance(resource)? getAssociatedEntryFields((CDAEntry)resource) :
        resource.rawFields())
      .collect(Collectors.toList());
  }

  /**
   * Associated entities are indexed using title, summary and id.
   */
  private static Map<String, Object> getAssociatedEntryFields(CDAEntry cdaEntry) {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put(ID_FIELD, cdaEntry.id());
    fields.putAll(cdaEntry.rawFields().entrySet().stream()
                    .filter(entry -> LINKED_ENTRY_FIELDS.matcher(entry.getKey()).matches()
                                     && cdaEntry.getField(entry.getKey()) != null)  //a project had a null url value
                    .collect(Collectors.toMap(Map.Entry::getKey,
                                              entry -> isLocalized(entry.getKey(), cdaEntry.contentType())
                                                ? entry.getValue()
                                                : cdaEntry.getField(entry.getKey()))));
    return fields;
  }

  /**
   * Is the fieldName localized in the CDAContentType.
   */
  private static boolean isLocalized(String fieldName, CDAContentType cdaContentType) {
    return ContentTypeFields.of(cdaContentType).getField(fieldName).isLocalized();
  }

}
