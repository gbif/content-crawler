package org.gbif.content.crawl.contentful.crawl;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


import com.contentful.java.cda.CDAClient;
import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAContentType;
import com.google.common.base.Preconditions;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.es.ElasticSearchUtils.buildEsClient;


/**
 * Pulls content from Contentful and stores it in ElasticSearch indexes.
 */
public class ContentfulCrawler {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulCrawler.class);

  //3 Minutes
  private static final int CONNECTION_TO =  3;

  private final ContentCrawlConfiguration.Contentful configuration;
  private final CDAClient cdaClient;
  private final CMAClient cmaClient;
  private final Client esClient;

  //News Content Type Id
  private String newsContentTypeId;

  /**
   * ElasticSearch and Contentful configuration are required to create an instance of this class.
   */
  public ContentfulCrawler(ContentCrawlConfiguration configuration) {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.elasticSearch, "ElasticSearch configuration can't be null");
    Preconditions.checkNotNull(configuration.contentful, "Contentful configuration can't be null");
    this.configuration = configuration.contentful;
    cdaClient = buildCdaClient();
    cmaClient = buildCmaClient();
    esClient = buildEsClient(configuration.elasticSearch);
  }


  /**
   * Executes the Crawler in a synchronous way, i.e.: process each resource per content type sequentially.
   */
  public void run() {
    LOG.info("Starting Contentful crawling");
    //Partition of ContentTypes: TRUE: vocabularies and FALSE: non-vocabularies/web content
    Map<Boolean, List<CMAContentType>> contentTypes = getContentTypes();
    List<CMAContentType> vocContentTypes = contentTypes.get(Boolean.TRUE);
    List<CMAContentType> webContentTypes = contentTypes.get(Boolean.FALSE);

    //Extract vocabularies meta data
    VocabularyTerms vocabularyTerms =  getVocabularyTerms(vocContentTypes);

    //Mapping generator can be re-used for all content types
    MappingGenerator mappingGenerator = new MappingGenerator(vocContentTypes);

    //Gets the News ContentType.resourceId
    newsContentTypeId = getNewsContentTypeId(webContentTypes);

    //Crawl all Content Types, except for vocabularies
    crawlContentTypes(webContentTypes, mappingGenerator, vocabularyTerms);
    LOG.info("Contentful crawling has finished");
  }

  /**
   * Extracts the terms of content types that represent vocabularies.
   */
  private VocabularyTerms getVocabularyTerms(Collection<CMAContentType> vocabularies) {
    VocabularyTerms vocabularyTerms =  new VocabularyTerms();
    vocabularies
      .forEach(contentType -> {
        //Keeps the country vocabulary ID for future use
        if (contentType.getName().equals(configuration.countryVocabulary)) {
          vocabularyTerms.loadCountryVocabulary(contentType);
        } else {
          //Loads vocabulary into memory
          vocabularyTerms.loadVocabulary(contentType);
        }
      });
    return vocabularyTerms;
  }

  /**
   * Gets the ID of the news content type.
   */
  private String getNewsContentTypeId(Collection<CMAContentType> contentTypes) {
    return contentTypes.stream()
      .filter(contentType -> contentType.getName().equals(configuration.newsContentType))
      .findFirst()
      .orElseThrow(() -> new IllegalStateException("News ContentType not found")).getResourceId();
  }

  /**
   * Crawls a list of ContentTypes into ElasticSearch.
   */
  private void crawlContentTypes(Collection<CMAContentType> contentTypes, MappingGenerator mappingGenerator,
                                 VocabularyTerms vocabularyTerms) {
    //The stream is sorted to ensure that the News content type is crawled first, due that it might be updated
    //to store reverse links into it
    contentTypes.stream()
      .filter(contentType -> configuration.contentTypes.contains(contentType.getName()))
      .sorted((ct1,ct2) -> Integer.compare(configuration.contentTypes.indexOf(ct1.getName()),
                                           configuration.contentTypes.indexOf(ct2.getName())))
      .forEach(contentType -> {
        ContentTypeCrawler contentTypeCrawler = new ContentTypeCrawler(contentType, mappingGenerator, esClient,
                                                                       configuration, cdaClient,
                                                                       vocabularyTerms,
                                                                       newsContentTypeId);
        contentTypeCrawler.crawl();
      });
  }


  /**
   * @return a new instance of a Contentful CDAClient.
   */
  private CDAClient buildCdaClient() {
     CDAClient.Builder builder = CDAClient.builder();
    return builder
            .setCallFactory(builder.defaultCallFactoryBuilder().readTimeout(CONNECTION_TO, TimeUnit.MINUTES).retryOnConnectionFailure(true).build())
            .setSpace(configuration.spaceId).setToken(configuration.cdaToken).build();
  }

  /**
   * @return a new instance of a Contentful CDAClient.
   */
  private CMAClient buildCmaClient() {
    return new CMAClient.Builder().setAccessToken(configuration.cmaToken).build();
  }


  /**
   * @return a partition of ContentTypes split into sets:  TRUE: vocabularies and FALSE: non-vocabularies/web content.
   */
  private Map<Boolean,List<CMAContentType>> getContentTypes() {
    Collection<String> allContentTypes = new LinkedHashSet<>(configuration.vocabularies);
    allContentTypes.addAll(configuration.contentTypes);
    allContentTypes.add(configuration.newsContentType);
    return cmaClient.contentTypes().fetchAll(configuration.spaceId)
            .getItems().stream().filter(contentType -> allContentTypes.contains(contentType.getName()))
            .collect(Collectors.partitioningBy(contentType -> configuration.vocabularies
                                                                .contains(contentType.getName())));
  }

}
