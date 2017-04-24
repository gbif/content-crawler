package org.gbif.content.crawl.contentful;

import org.gbif.content.crawl.VocabularyTerms;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;
import org.gbif.content.crawl.contentful.meta.Meta;
import org.gbif.content.crawl.es.ElasticSearchUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.CDAField;
import com.contentful.java.cda.LocalizedResource;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAField;
import io.reactivex.Observable;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.contentful.MappingGenerator.COLLAPSIBLE_FIELDS;
import static org.gbif.content.crawl.contentful.MappingGenerator.COLLAPSIBLE_TYPES;

/**
 * Crawls a single contentful content type.
 */
public class ContentTypeCrawler {

  private static final Logger LOG = LoggerFactory.getLogger(ContentTypeCrawler.class);

  private static final Pattern LINKED_ENTRY_FIELDS = Pattern.compile(".*summary.*|.*title.*|label|url");

  private static final String CONTENT_TYPE_FIELD = "contentType";

  private static final String REGION_FIELD = "gbifRegion";


  private static final String ID_FIELD = "id";

  private static final int PAGE_SIZE = 20;

  private final Set<String> collapsedFields;

  private final CMAContentType contentType;

  private final String esIdxName;

  private final String esTypeName;

  //News Content Type Id
  private final NewsLinker newsLinker;

  private final MappingGenerator mappingGenerator;
  private final Client esClient;
  private final CDAClient cdaClient;
  private final ContentCrawlConfiguration configuration;
  private final VocabularyTerms vocabularyTerms;

  public ContentTypeCrawler(CMAContentType contentType,
                            MappingGenerator mappingGenerator,
                            Client esClient,
                            ContentCrawlConfiguration configuration,
                            CDAClient cdaClient,
                            VocabularyTerms vocabularyTerms,
                            String newsContentTypeId) {
    this.contentType = contentType;
    collapsedFields = getCollapsedFields(contentType);
    //index name has to be in lowercase
    esIdxName = ElasticSearchUtils.getEsIdxName(contentType.getName());
    //ES type name for this content typ
    esTypeName = ElasticSearchUtils.toFieldNameFormat(contentType.getName());
    //Used to create links in the news index
    newsLinker = new NewsLinker(newsContentTypeId, esClient, configuration.contentful.indexBuild.esIndexType);

    //Set the mapping generator
    this.mappingGenerator = mappingGenerator;

    this.esClient = esClient;

    this.configuration = configuration;

    this.cdaClient = cdaClient;

    this.vocabularyTerms = vocabularyTerms;
  }

  /**
   * Crawls the assigned content type into ElasticSearch.
   */
  public void crawl() {
    //gets or (re)create the ES idx if doesn't exists
    createIndex();
    LOG.info("Indexing ContentType [{}] into ES Index [{}]", contentType.getName(), esIdxName);
    //Prepares the bulk/batch request
    BulkRequestBuilder bulkRequest = esClient.prepareBulk();
    //Retrieves resources in a CDAArray
    Observable.fromIterable(new ContentfulPager(cdaClient, PAGE_SIZE, contentType.getResourceId()))
      .doOnComplete(() -> executeBulkRequest(bulkRequest))
      .subscribe(results -> results.items()
        .forEach(cdaResource ->
                   bulkRequest.add(esClient.prepareIndex(esIdxName.toLowerCase(),
                                                         configuration.contentful.indexBuild.esIndexType,
                                                         cdaResource.id())
                                     .setSource(getIndexedFields((CDAEntry)cdaResource)))
        ));
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
  private List<Map<String,Object>> toListValues(List<LocalizedResource> resources, boolean localized) {
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
          VocabularyBuilder vocabularyBuilder = new VocabularyBuilder(vocabularyTerms);
          vocabularyBuilder.of(cdaEntry.getField(field))
            .one(vocValue -> entries.put(field, vocValue))
            .gbifRegion(gbifRegion -> entries.put(REGION_FIELD, gbifRegion));
          if(vocabularyBuilder.isEmpty()) {
            CDAEntry fieldCdaEntry = cdaEntry.getField(field);
            newsLinker.processNewsTag(fieldCdaEntry, esTypeName, cdaEntry.id());
            entries.put(field, getAssociatedEntryFields(fieldCdaEntry));
          }
        }
      } else if(ContentfulType.ARRAY == fieldType) {
        VocabularyBuilder vocabularyBuilder = new VocabularyBuilder(vocabularyTerms);
        vocabularyBuilder.ofList(cdaEntry.getField(field))
          .all(vocValues -> entries.put(field, vocValues))
          .allGbifRegions(gbifRegions -> entries.put(REGION_FIELD, gbifRegions));
        if(vocabularyBuilder.isEmpty()) {
          List<LocalizedResource> fieldCdaEntries = cdaEntry.getField(field);
          newsLinker.processNewsTag(fieldCdaEntries, esTypeName, cdaEntry.id());
          entries.put(field, toListValues(fieldCdaEntries, cdaField.isLocalized()));
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
      fields.putAll(cdaEntry.rawFields().entrySet().stream()
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
  private static Set<String> getCollapsedFields(CMAContentType contentType) {
    return contentType.getFields().stream()
      .filter(cdaField -> COLLAPSIBLE_TYPES.contains(cdaField.getType())
                          || COLLAPSIBLE_FIELDS.matcher(cdaField.getId()).matches())
      .map(CMAField::getId).collect(Collectors.toSet());
  }

  /**
   * Creates, if doesn't exists, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  private void createIndex() {
    //create ES idx if it doesn't exists
    if (!esClient.admin().indices().prepareExists(esIdxName).get().isExists()) {
      esClient.admin().indices().prepareCreate(esIdxName)
        .addMapping(configuration.contentful.indexBuild.esIndexType, mappingGenerator.getEsMapping(contentType)).get();
    } else if (configuration.contentful.indexBuild.deleteIndex) { //if the index exists and should be recreated
      //Delete the index
      esClient.admin().indices().prepareDelete(esIdxName).get();
      //Re-create the index
      esClient.admin().indices().prepareCreate(esIdxName)
        .addMapping(configuration.contentful.indexBuild.esIndexType, mappingGenerator.getEsMapping(contentType)).get();
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

}
