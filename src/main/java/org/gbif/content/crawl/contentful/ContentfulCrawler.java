package org.gbif.content.crawl.contentful;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    cdaClient = cdaClient();
    esClient = esClient();
    vocabularies = new HashMap<>();
  }

  /**
   * Executes the Crawler in a synchronous way, i.e.: process each resource per content type sequentially.
   */
  public void run() {
    getContentTypes().items().parallelStream().map(cdaResource -> (CDAContentType)cdaResource)
      .forEach(contentType -> {
        String idxName = REPLACEMENTS.matcher(contentType.name().toLowerCase()).replaceAll(""); //index name has to be in lowercase
        //Loads vocabulary into memory
        if (idxName.startsWith(VOCABULARY_KEYWORD)) {
          VocabularyLoader.vocabularyTerms(contentType.id(), cdaClient)
            .subscribe(terms -> vocabularies.put(idxName, terms));
        } else {
          //gets or (re)create the ES idx if doesn't exists
          createRetrieveIdx(contentType.id(), idxName);
          LOG.info("Indexing ContentType [{}] into ES Index [{}]", contentType.name(), idxName);
          //Prepares the bulk/batch request
          BulkRequestBuilder bulkRequest = esClient.prepareBulk();
          //Retrieves resources in a CDAArray
          Observable.fromIterable(new ContentfulPager(cdaClient, PAGE_SIZE, contentType.id()))
            .doOnComplete(() -> executeBulkRequest(bulkRequest, contentType.id()))
            .subscribe(results -> results.items()
                                    .forEach(cdaResource ->
                                      bulkRequest.add(esClient.prepareIndex(idxName,
                                                                            configuration.contentful.esIndexType,
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
    //Add meta attributes
    indexedFields.putAll(cdaEntry.attrs());
    //Process vocabularies
    indexedFields.entrySet().stream()
      .filter(entry -> entry.getKey().toLowerCase().startsWith(VOCABULARY_KEYWORD))
      //replace vocabularies with localized values taken from the in-memory structure
      .forEach(entry -> indexedFields.replace(entry.getKey(), getVocabularyValues(entry.getKey(), cdaEntry)));
    return indexedFields;
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
  private Map<String,List<String>> getVocabularyValues(String vocabularyName, CDAEntry cdaEntry) {
    Map<String,List<Map<String,Map<String, String>>>> vocabulary =
      (Map<String,List<Map<String,Map<String, String>>>>)cdaEntry.rawFields().get(vocabularyName);
    Map<String,List<String>> vocabularyEntries = new HashMap<>();
    vocabulary.forEach((locale, values) ->
      vocabularyEntries.put(locale, values.stream().map(i8NValues -> vocabularies.get(vocabularyName.toLowerCase())
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
   * Creates an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  private void createRetrieveIdx(String contentTypeId, String idxName) {

      //create ES idx if it doesn't exists
      if (!esClient.admin().indices().prepareExists(idxName).get().isExists()) {
        esClient.admin().indices().prepareCreate(idxName)
          .addMapping(configuration.contentful.esIndexType, indexMappings()).get();
      } else if (configuration.contentful.deleteIndex) { //if the index exists and should be recreated
        //Delete the index
        esClient.admin().indices().prepareDelete(idxName).get();
        //Re-create the index
        esClient.admin().indices().prepareCreate(idxName).get();
      }

  }

  /**
   * Reads the content of ES_MAPPINGS_FILE into a String.
   */
  private String indexMappings() {
    try {
      return new String (IOUtils.toByteArray(Thread.currentThread().getContextClassLoader()
                                               .getResourceAsStream(ES_MAPPINGS_FILE)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Creates a new instance of a Contentful CDAClient.
   */
  private CDAClient cdaClient() {
    return CDAClient.builder().setSpace(configuration.contentful.spaceId)
      .setToken(configuration.contentful.cdaToken).build();
  }

  /**
   * Creates a new instance of a ElasticSearch client.
   */
  private Client esClient() {
    try {
      Settings settings = Settings.builder().put("cluster.name", configuration.elasticSearch.cluster).build(); // important
      return new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(
        configuration.elasticSearch.host), configuration.elasticSearch.port));
    } catch (UnknownHostException ex) {
      throw new IllegalStateException(ex);
    }
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
