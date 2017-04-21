package org.gbif.content.crawl.contentful;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;
import org.gbif.content.crawl.contentful.meta.Meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.contentful.java.cda.CDAAsset;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.CDAField;
import com.contentful.java.cda.LocalizedResource;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Sets;
import io.reactivex.Observable;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.contentful.MappingGenerator.COLLAPSIBLE_FIELDS;
import static org.gbif.content.crawl.contentful.MappingGenerator.COLLAPSIBLE_TYPES;

/**
 * Crawls a single contentful content type.
 */
public class ContentTypeCrawler {

  private static final Logger LOG = LoggerFactory.getLogger(ContentTypeCrawler.class);

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  private static final Pattern LINKED_ENTRY_FIELDS = Pattern.compile(".*summary.*|.*title.*|label|url");

  private static final String CONTENT_TYPE_FIELD = "contentType";

  private static final String REGION_FIELD = "gbifRegion";

  private static final String NEWS_UPDATE_SCRIPT  = "if (ctx._source.%1$s == null) {ctx._source.%1$s = [params.tag]} else if (ctx._source.%1$s.contains(params.tag)) { ctx.op = \"none\"  } else { ctx._source.%1$s.add(params.tag) }";


  private static final String ID_FIELD = "id";

  private static final int PAGE_SIZE = 20;

  private final Set<String> collapsedFields;

  private CDAContentType cdaContentType;

  private final String esIdxName;

  private final String esTypeName;

  //Country Content Type Id
  private final String countryContentTypeId;

  //News Content Type Id
  private final String newsContentTypeId;

  private final MappingGenerator mappingGenerator;
  private final Client esClient;
  private final CDAClient cdaClient;
  private final ContentCrawlConfiguration configuration;
  /**
   * vocabularyName -> { contentId -> defaultValue} }
   */
  private final Set<String> vocabulariesContentTypeIds;

  public ContentTypeCrawler(CDAContentType cdaContentType, MappingGenerator mappingGenerator,
                            Client esClient, ContentCrawlConfiguration configuration,
                            CDAClient cdaClient,
                            Set<String> vocabulariesContentTypeIds,
                            String countryContentTypeId,
                            String newsContentTypeId) {
    this.cdaContentType = cdaContentType;
    collapsedFields = getCollapsedFields(cdaContentType);
    //index name has to be in lowercase
    esIdxName = getEsIdxName(cdaContentType);
    //ES type name for this content typ
    esTypeName = toLowerCamel(cdaContentType.name());
    //Set the mapping generator
    this.mappingGenerator = mappingGenerator;

    this.esClient = esClient;

    this.configuration = configuration;

    this.cdaClient = cdaClient;

    this.vocabulariesContentTypeIds = vocabulariesContentTypeIds;

    this.countryContentTypeId = countryContentTypeId;

    this.newsContentTypeId = newsContentTypeId;
  }

  public void crawl() {
    //gets or (re)create the ES idx if doesn't exists
    createIndex();
    LOG.info("Indexing ContentType [{}] into ES Index [{}]", cdaContentType.name(), esIdxName);
    //Prepares the bulk/batch request
    BulkRequestBuilder bulkRequest = esClient.prepareBulk();
    //Retrieves resources in a CDAArray
    Observable.fromIterable(new ContentfulPager(cdaClient, PAGE_SIZE, cdaContentType.id()))
      .doOnComplete(() -> executeBulkRequest(bulkRequest))
      .subscribe(results -> results.items()
        .forEach(cdaResource ->
                   bulkRequest.add(esClient.prepareIndex(esIdxName.toLowerCase(),
                                                         configuration.contentful.indexBuild.esIndexType,
                                                         cdaResource.id())
                                     .setSource(getIndexedFields((CDAEntry)cdaResource)))
        ));
  }

  private static String getEsIdxName(CDAContentType contentType) {
    return REPLACEMENTS.matcher(contentType.name()).replaceAll("").toLowerCase();
  }

  /**
   * Extracts the fields that will be indexed in ElasticSearch.
   */
  private Map<String,Object> getIndexedFields(CDAEntry cdaEntry) {
    //Add all rawFields
    Map<String, Object> indexedFields = fieldsFromEntry(cdaEntry);
    //Add meta attributes
    indexedFields.putAll(cdaEntry.attrs());
    indexedFields.put(CONTENT_TYPE_FIELD, esTypeName);
    //Updates the information from the meta field
    Meta.getMetaCreatedDate(cdaEntry).ifPresent(createdDate -> indexedFields.replace("createdAt", createdDate));
    return indexedFields;
  }

  /**
   * Gets the content of an asset.
   */
  private static Map<String, Object> getNestedContent(LocalizedResource resource, boolean localized) {
    if(resource != null) {
      return localized
        ? resource.rawFields()
        : resource.rawFields()
          .entrySet()
          .stream()
          .filter(entry -> resource.getField(entry.getKey()) != null)
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> resource.getField(entry.getKey())));
    }
   return new HashMap<>();
  }

  /**
   * Extract the values as maps of the list of resources.
   */
  private static List<Map<String,Object>> toListValues(List<LocalizedResource> resources, boolean localized) {
    return resources.stream()
      .map(resource -> CDAEntry.class.isInstance(resource)? getAssociatedEntryFields((CDAEntry)resource) :
                                                            getNestedContent(resource, localized)
    ).collect(Collectors.toList());
  }

  /**
   * Iterates trough each Asset entry in cdaEntry and retrieves its value.
   */
  private  Map<String,Object> fieldsFromEntry(CDAEntry cdaEntry) {
    Map<String,Object> entries = new HashMap<>();
    ContentTypeFields contentTypeFields = ContentTypeFields.of(cdaEntry.contentType());
    cdaEntry.rawFields().forEach((field,value) -> {
      CDAField cdaField = contentTypeFields.getField(field);
      ContentfulType fieldType = contentTypeFields.getFieldType(field);
      if(ContentfulType.LINK == fieldType) {
        if(ContentfulLinkType.ASSET == contentTypeFields.getFieldLinkType(field)) {
          entries.put(field, getNestedContent(cdaEntry.getField(field), cdaField.isLocalized()));
        } else {
          VocabularyCollector vocabularyCollector = new VocabularyCollector(vocabulariesContentTypeIds, countryContentTypeId);
          vocabularyCollector.of(cdaEntry.getField(field))
            .one(vocValue -> entries.put(field, vocValue))
            .gbifRegion(gbifRegion -> entries.put(REGION_FIELD, gbifRegion));
          if(vocabularyCollector.isEmpty()) {
            entries.put(field, getAssociatedEntryFields(cdaEntry.getField(field)));
          }
        }
      } else if(ContentfulType.ARRAY == fieldType) {
        VocabularyCollector vocabularyCollector = new VocabularyCollector(vocabulariesContentTypeIds, countryContentTypeId);
        vocabularyCollector.ofList(cdaEntry.getField(field))
          .all(vocValues -> entries.put(field, vocValues))
          .allGbifRegions(gbifRegions -> entries.put(REGION_FIELD, gbifRegions));
        if(vocabularyCollector.isEmpty()) {
          entries.put(field, toListValues(cdaEntry.getField(field), cdaField.isLocalized()));
        }
      } else {
        entries.put(field, cdaField.isLocalized() && !collapsedFields.contains(field)? value : cdaEntry.getField(field));
      }
    });
    return entries;
  }

  /**
   * Associated entities are indexed using title, summary and id.
   */
  private static Map<String, Object> getAssociatedEntryFields(CDAEntry cdaEntry) {
    Map<String, Object> fields = new LinkedHashMap<>();
    if(cdaEntry != null) {
      fields.put(ID_FIELD, cdaEntry.id());
      fields.putAll(cdaEntry.rawFields()
                      .entrySet()
                      .stream()
                      .filter(entry -> LINKED_ENTRY_FIELDS.matcher(entry.getKey()).matches()
                                       && cdaEntry.getField(entry.getKey()) != null)  //a project had a null url value
                      .collect(Collectors.toMap(Map.Entry::getKey,
                                                entry -> isLocalized(entry.getKey(), cdaEntry.contentType())
                                                  ? entry.getValue()
                                                  : cdaEntry.getField(entry.getKey()))));
    }
    return fields;
  }

  /**
   * Is the fieldName localized in the CDAContentType.
   */
  private static boolean isLocalized(String fieldName, CDAContentType cdaContentType) {
    return ContentTypeFields.of(cdaContentType).getField(fieldName).isLocalized();
  }


  /**
   *
   * Returns a Set of the fields that can be get straight from an entry instead of getting its localized version.
   */
  private static Set<String> getCollapsedFields(CDAContentType contentType) {
    return contentType.fields().stream()
      .filter(cdaField -> COLLAPSIBLE_TYPES.matcher(cdaField.type()).matches()
                          || COLLAPSIBLE_FIELDS.matcher(cdaField.id()).matches())
      .map(CDAField::id).collect(Collectors.toSet());
  }

  /**
   * Translates a sentence type text into upper camel case format.
   * For example: "Hola Morten" will be transformed into "holaMorten".
   */
  private static String toLowerCamel(CharSequence sentence) {
    return CaseFormat.UPPER_UNDERSCORE
      .to(CaseFormat.LOWER_CAMEL, REPLACEMENTS.matcher(sentence).replaceAll("_").toUpperCase());
  }

  /**
   * Creates, if doesn't exists, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  private void createIndex() {
    //create ES idx if it doesn't exists
    if (!esClient.admin().indices().prepareExists(esIdxName).get().isExists()) {
      esClient.admin().indices().prepareCreate(esIdxName)
        .addMapping(configuration.contentful.indexBuild.esIndexType, mappingGenerator.getEsMapping(cdaContentType)).get();
    } else if (configuration.contentful.indexBuild.deleteIndex) { //if the index exists and should be recreated
      //Delete the index
      esClient.admin().indices().prepareDelete(esIdxName).get();
      //Re-create the index
      esClient.admin().indices().prepareCreate(esIdxName)
        .addMapping(configuration.contentful.indexBuild.esIndexType, mappingGenerator.getEsMapping(cdaContentType)).get();
    }
  }

  /**
   * Performs the execution of a ElasticSearch BulkRequest and logs the correspondent results.
   */
  private void executeBulkRequest(BulkRequestBuilder bulkRequest) {
    if (bulkRequest.numberOfActions() > 0) {
      BulkResponse bulkResponse = bulkRequest.get();
      if (bulkResponse.hasFailures()) {
        LOG.error("Error indexing.  First error message: {}", bulkResponse.getItems()[0].getFailureMessage());
      } else {
        LOG.info("Indexed [{}] documents of content type [{}]", bulkResponse.getItems().length, esIdxName);
      }
    } else  {
      LOG.info("Nothing to index for content type [{}]", esIdxName);
    }
  }

  /**
   * Processes the news associated to an entry.
   * This method updates the news index by adding a new field [contentTypeName]Tag which stores all the ids
   * related to that news from this content type, it is used for creating RSS feeds for specific elements.
   */
  private void processNewsTag(CDAEntry cdaEntry, String tagValue) {
    if (cdaEntry.contentType().id().equals(newsContentTypeId)) {
      try {
        Map<String, Object> params = new HashMap<>();
        params.put("tag", tagValue);
        String scriptText = String.format(NEWS_UPDATE_SCRIPT, esTypeName + "Tag");
        Script script = new Script(ScriptType.INLINE, "painless", scriptText, params);
        esClient.prepareUpdate(getEsIdxName(cdaEntry.contentType()),
                               configuration.contentful.indexBuild.esIndexType,
                               cdaEntry.id()).setScript(script).get();
      } catch (Exception ex) {
        LOG.error("Error updating news tag {} from entry {} ", tagValue, cdaEntry, ex);
      }
    }
  }
}
