package org.gbif.content.crawl.contentful;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.FetchQuery;
import com.google.common.base.Preconditions;
import io.reactivex.Observable;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.es.ElasticSearchUtils.buildEsClient;
import static org.gbif.content.crawl.es.ElasticSearchUtils.createIndex;

/**
 * Pulls content from Contentful and stores it in ElasticSearch indexes.
 */
public class ContentfulCrawler {

  private static final String ES_MAPPINGS_FILE  = "contentful_mapping.json";
  private static final Logger LOG = LoggerFactory.getLogger(ContentfulCrawler.class);

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  private static final String VOCABULARY_KEYWORD = "vocabulary";

  private final ContentCrawlConfiguration configuration;
  private final CDAClient cdaClient;
  private final Client esClient;

  /**
   * vocabularyName -> { contentId -> {locale -> termLocalizedValue} }
   */
  private final Map<String,Map<String,Map<String,String>>> vocabularies;

  private static final int PAGE_SIZE = 20;

  /**
   * ElasticSearch and Contentful configuration are required to create an instance of this class.
   */
  public ContentfulCrawler(ContentCrawlConfiguration configuration) {
    Preconditions.checkNotNull(configuration, "Crawler configruration can't be null");
    Preconditions.checkNotNull(configuration.elasticSearch, "ElasticSearch configruration can't be null");
    Preconditions.checkNotNull(configuration.contentful, "Contentful configruration can't be null");
    this.configuration = configuration;
    cdaClient = buildCdaClient();
    esClient = buildEsClient(configuration.elasticSearch);
    vocabularies = new HashMap<>();
  }

  /**
   * Executes the Crawler in a synchronous way, i.e.: process each resource per content type sequentially.
   */
  public void run() {
    getContentTypes().items().stream().map(cdaResource -> (CDAContentType)cdaResource)
      .forEach(contentType -> {
        //index name has to be in lowercase
        String idxName = REPLACEMENTS.matcher(contentType.name().toLowerCase()).replaceAll("");
        //Loads vocabulary into memory
        if (idxName.startsWith(VOCABULARY_KEYWORD)) {
          VocabularyLoader.vocabularyTerms(contentType.id(), cdaClient)
            .subscribe(terms -> vocabularies.put(contentType.id(), terms));
        }
      });

    getContentTypes().items().stream().map(cdaResource -> (CDAContentType)cdaResource)
      .forEach(contentType -> {
        //index name has to be in lowercase
        String idxName = REPLACEMENTS.matcher(contentType.name().toLowerCase()).replaceAll("");
        //Loads vocabulary into memory
        if (!idxName.startsWith(VOCABULARY_KEYWORD)) {
          //gets or (re)create the ES idx if doesn't exists
          createIndex(esClient, configuration.contentful.indexBuild, idxName, ES_MAPPINGS_FILE);
          LOG.info("Indexing ContentType [{}] into ES Index [{}]", contentType.name(), idxName);
          //Prepares the bulk/batch request
          BulkRequestBuilder bulkRequest = esClient.prepareBulk();
          //Retrieves resources in a CDAArray
          Observable.fromIterable(new ContentfulPager(cdaClient, PAGE_SIZE, contentType.id()))
            .doOnComplete(() -> executeBulkRequest(bulkRequest, contentType.id()))
            .subscribe(results -> results.items()
              .forEach(cdaResource ->
                         bulkRequest.add(esClient.prepareIndex(idxName,
                                                               configuration.contentful.indexBuild.esIndexType,
                                                               cdaResource.id())
                                           .setSource(getIndexedFields((CDAEntry) cdaResource)))
              ));
        }
      });
  }

  /**
   * Extracts the fields that will be indexed in ElasticSearch.
   */
  private Map<String,Object> getIndexedFields(CDAEntry cdaEntry) {
    //Add all rawFields
    Map<String, Object> indexedFields = new HashMap<>(cdaEntry.rawFields());
    //Process vocabularies
    cdaEntry.rawFields().entrySet().stream()
      .filter(entry -> isVocabulary(cdaEntry, entry.getKey()))
      //replace vocabularies with localized values taken from the in-memory structure
      .forEach(entry -> indexedFields.replace(entry.getKey(), getVocabularyValues(entry.getKey(), cdaEntry)));
    //Add meta attributes
    indexedFields.putAll(cdaEntry.attrs());
    return indexedFields;
  }

  private boolean isVocabulary(CDAEntry cdaEntry, String field) {
    return List.class.isAssignableFrom(cdaEntry.getField(field).getClass())
           && CDAEntry.class.isInstance(((List)cdaEntry.getField(field)).get(0))
           && vocabularies.containsKey(((CDAEntry)((List)cdaEntry.getField(field)).get(0)).contentType().id());
  }

  /**
   * Extracts the vocabulary term values.
   * This methods navigates thru a JSON structure like:
   * "vocabularyTopic": {
   *    "en-US": [
   *    {
   *      "sys": {
   *        "type": "Link",
   *        "linkType": "Entry",
   *        "id": "2dANcb05ZymogmGSQI0GAG"
   *      }
   *    }....
   *    ]
   * }
   */
  private Map<String,List<String>> getVocabularyValues(String vocabularyField, CDAEntry cdaEntry) {
    String vocabularyContentTypeId = ((CDAEntry)((List)cdaEntry.getField(vocabularyField)).get(0)).contentType().id();
    Map<String,List<Map<String,Map<String, String>>>> vocabulary =
      (Map<String,List<Map<String,Map<String, String>>>>)cdaEntry.rawFields().get(vocabularyField);
    Map<String,List<String>> vocabularyEntries = new HashMap<>();
    vocabulary.forEach((locale, values) ->
      vocabularyEntries.put(locale, values.stream().map(i8NValues -> vocabularies.get(vocabularyContentTypeId)
                                                                       .get(i8NValues.get("sys").get("id")).get(locale))
                                                  .collect(Collectors.toList())));
    return vocabularyEntries;
  }


  /**
   * Performs the execution of a ElasticSearch BulkRequest and logs the correspondent results.
   */
  private static void executeBulkRequest(BulkRequestBuilder bulkRequest, String contentTypeId) {
    if(bulkRequest.numberOfActions() > 0) {
      BulkResponse bulkResponse = bulkRequest.get();
      if (bulkResponse.hasFailures()) {
        LOG.error("Error indexing.  First error message: {}", bulkResponse.getItems()[0].getFailureMessage());
      } else {
        LOG.info("Indexed [{}] documents of content type [{}]", bulkResponse.getItems().length, contentTypeId);
      }
    } else  {
      LOG.info("Nothing to index for content type [{}]", contentTypeId);
    }
  }

  /**
   * Creates a new instance of a Contentful CDAClient.
   */
  private CDAClient buildCdaClient() {
    return CDAClient.builder().setSpace(configuration.contentful.spaceId)
      .setToken(configuration.contentful.cdaToken).build();
  }


  /**
   * CDAArray that holds references to the list of content types defined in configuration.contentful.contentTypes.
   * If configuration.contentful.contentTypes is empty, all content types are returned.
   */
  private CDAArray getContentTypes() {
    FetchQuery<CDAContentType> fetchQuery = cdaClient.fetch(CDAContentType.class);
    for(String contentType : configuration.contentful.contentTypes) {
      fetchQuery = fetchQuery.withContentType(contentType);
    }
    return fetchQuery.all();
  }

}
