package org.gbif.content.crawl.contentful;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAAsset;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.google.common.base.CaseFormat;
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

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulCrawler.class);

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  private static final String CONTENT_TYPE_FIELD = "contentType";

  private static final int PAGE_SIZE = 20;

  private final ContentCrawlConfiguration configuration;
  private final CDAClient cdaClient;
  private final Client esClient;


  /**
   * vocabularyName -> { contentId -> defaultValue} }
   */
  private final Map<String,Map<String, String>> vocabularies;

  /**
   * ElasticSearch and Contentful configuration are required to create an instance of this class.
   */
  public ContentfulCrawler(ContentCrawlConfiguration configuration) {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.elasticSearch, "ElasticSearch configuration can't be null");
    Preconditions.checkNotNull(configuration.contentful, "Contentful configuration can't be null");
    this.configuration = configuration;
    cdaClient = buildCdaClient();
    esClient = buildEsClient(configuration.elasticSearch);
    vocabularies = new HashMap<>();
  }

  /**
   * Translates a sentence type text into upper camel case format.
   * For example: "Hola Morten" will be transformed into "holaMorten".
   */
  private static String toLowerCamel(String sentence) {
    return CaseFormat.UPPER_UNDERSCORE
            .to(CaseFormat.LOWER_CAMEL, REPLACEMENTS.matcher(sentence).replaceAll("_").toUpperCase());
  }

  /**
   * Executes the Crawler in a synchronous way, i.e.: process each resource per content type sequentially.
   */
  public void run() {
    List<CDAContentType> contentTypes = getContentTypes().items().stream()
                                          .map(cdaResource -> (CDAContentType)cdaResource).collect(Collectors.toList());
    Set<CDAContentType> vocContentTypes = contentTypes.stream()
      .filter(contentType -> configuration.contentful.vocabularies.contains(contentType.name()))
      .collect(Collectors.toSet());
    vocContentTypes.forEach(contentType -> {
      //Loads vocabulary into memory
      VocabularyLoader.vocabularyTerms(contentType.id(), cdaClient)
        .subscribe(terms -> vocabularies.put(contentType.id(), terms));
    });

    //Mapping generator can be re-used for all content types
    MappingGenerator mappingGenerator = new MappingGenerator(vocContentTypes);

    contentTypes.stream().filter(contentType -> configuration.contentful.contentTypes.contains(contentType.name()))
      .forEach(contentType -> {
        //index name has to be in lowercase
        String idxName = REPLACEMENTS.matcher(contentType.name()).replaceAll("").toLowerCase();

        //gets or (re)create the ES idx if doesn't exists
        createIndex(esClient, configuration.contentful.indexBuild, idxName, mappingGenerator.getEsMapping(contentType));
        LOG.info("Indexing ContentType [{}] into ES Index [{}]", contentType.name(), idxName);
        //Prepares the bulk/batch request
        BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        //Retrieves resources in a CDAArray
        Observable.fromIterable(new ContentfulPager(cdaClient, PAGE_SIZE, contentType.id()))
          .doOnComplete(() -> executeBulkRequest(bulkRequest, contentType.id()))
          .subscribe(results -> results.items()
            .forEach(cdaResource ->
                       bulkRequest.add(esClient.prepareIndex(idxName.toLowerCase(),
                                                             configuration.contentful.indexBuild.esIndexType,
                                                             cdaResource.id())
                                         .setSource(getIndexedFields((CDAEntry)cdaResource,
                                                                     toLowerCamel(contentType.name()))))
            ));
      });
  }

  /**
   * Extracts the fields that will be indexed in ElasticSearch.
   */
  private Map<String,Object> getIndexedFields(CDAEntry cdaEntry, String contentTypeName) {
    //Add all rawFields
    Map<String, Object> indexedFields = new HashMap<>(cdaEntry.rawFields());
    indexedFields.putAll(processAssets(cdaEntry));
    //Process vocabularies
    cdaEntry.rawFields().entrySet().stream()
      .filter(entry -> isVocabulary(cdaEntry, entry.getKey()))
      //replace vocabularies with localized values taken from the in-memory structure
      .forEach(entry -> indexedFields.replace(entry.getKey(), getVocabularyValues(entry.getKey(), cdaEntry)));
    //Add meta attributes
    indexedFields.putAll(cdaEntry.attrs());
    indexedFields.put(CONTENT_TYPE_FIELD, contentTypeName);
    return indexedFields;
  }

  /**
   * Iterates trough each Asset entry in cdaEntry and retrieves its value.
   */
  private  Map<String,Map<String,Object>> processAssets(CDAEntry cdaEntry) {
    Map<String,Map<String,Object>> assets = new HashMap<>();
    cdaEntry.rawFields().forEach((field,value) -> {
      Object fieldValue = cdaEntry.getField(field);
      if (CDAAsset.class.isInstance(fieldValue)) {
        assets.put(field, cdaClient.fetch(CDAAsset.class).one(((CDAAsset)fieldValue).id()).rawFields());
      } else if (List.class.isInstance(fieldValue) && (!((List)fieldValue).isEmpty()
                 && CDAAsset.class.isInstance(((List)fieldValue).get(0)))) {
        ((List<?>)fieldValue).forEach(cdaAsset -> assets.put(field, cdaClient.fetch(CDAAsset.class).one(((CDAAsset)cdaAsset).id()).rawFields()));
      }
    });
    return  assets;
  }

  /**
   * Checks if fields in a cdaEntry is GBIF vocabulary.
   */
  private boolean isVocabulary(CDAEntry cdaEntry, String field) {
    return Optional.ofNullable(cdaEntry.getField(field)).map(fieldValue ->
      (List.class.isAssignableFrom(fieldValue.getClass())
       && !((List)fieldValue).isEmpty()
       && CDAEntry.class.isInstance(((List)fieldValue).get(0))
       && vocabularies.containsKey(((CDAEntry)((List)fieldValue).get(0)).contentType().id()))
    ).orElse(Boolean.FALSE);
  }

  /**
   * Extracts the vocabulary term values.
   */
  private List<String> getVocabularyValues(String vocabularyField, CDAEntry cdaEntry) {
    List<CDAEntry> vocabulary = cdaEntry.getField(vocabularyField);
    return vocabulary.stream()
            .map(entry -> vocabularies.get(entry.contentType().id()).get(entry.id()))
            .collect(Collectors.toList());
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
    return cdaClient.fetch(CDAContentType.class).all();
  }

}
