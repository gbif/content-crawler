package org.gbif.content.crawl.contentful;

import org.gbif.content.crawl.VocabularyTerms;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import com.contentful.java.cda.CDAClient;
import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAArray;
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

  private final ContentCrawlConfiguration configuration;
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
    this.configuration = configuration;
    cdaClient = buildCdaClient();
    cmaClient = buildCmaClient();
    esClient = buildEsClient(configuration.elasticSearch);
  }


  /**
   * Executes the Crawler in a synchronous way, i.e.: process each resource per content type sequentially.
   */
  public void run() {
    LOG.info("Starting Contentful crawling");
    VocabularyTerms vocabularyTerms =  new VocabularyTerms();
    List<CMAContentType> contentTypes = getContentTypes().getItems();
    Set<CMAContentType> vocContentTypes = contentTypes.stream()
      .filter(contentType -> configuration.contentful.vocabularies.contains(contentType.getName()))
      .collect(Collectors.toSet());
    vocContentTypes.forEach(contentType -> {
      //Keeps the country vocabulary ID for future use
      if (contentType.getName().equals(configuration.contentful.countryVocabulary)) {
        vocabularyTerms.loadCountryVocabulary(contentType);
      } else {
        //Loads vocabulary into memory
        vocabularyTerms.loadVocabulary(contentType);
      }
    });

    //Mapping generator can be re-used for all content types
    MappingGenerator mappingGenerator = new MappingGenerator(vocContentTypes);
    newsContentTypeId = contentTypes.stream()
                          .filter(contentType -> contentType.getName().equals(configuration.contentful.newsContentType))
                          .findFirst()
                          .orElseThrow(() -> new IllegalStateException("News ContentType not found")).getResourceId();
    contentTypes.stream()
      .filter(contentType -> configuration.contentful.contentTypes.contains(contentType.getName()))
      .sorted((contentType1, contentType2) -> Integer.compare(configuration.contentful.contentTypes.indexOf(contentType1.getName()),
                                                              configuration.contentful.contentTypes.indexOf(contentType2.getName())))
      .forEach(contentType -> {
        ContentTypeCrawler contentTypeCrawler = new ContentTypeCrawler(contentType, mappingGenerator, esClient,
                                                                       configuration, cdaClient, vocabularyTerms,
                                                                       newsContentTypeId);
        contentTypeCrawler.crawl();
    });
    LOG.info("Contentful crawling has finished");
  }


  /**
   * Creates a new instance of a Contentful CDAClient.
   */
  private CDAClient buildCdaClient() {
    return CDAClient.builder().setSpace(configuration.contentful.spaceId)
      .setToken(configuration.contentful.cdaToken).build();
  }

  /**
   * Creates a new instance of a Contentful CDAClient.
   */
  private CMAClient buildCmaClient() {
    return new CMAClient.Builder().setAccessToken(configuration.contentful.cmaToken).build();
  }


  /**
   * CDAArray that holds references to the list of content types defined in configuration.contentful.contentTypes.
   * If configuration.contentful.contentTypes is empty, all content types are returned.
   */
  private CMAArray<CMAContentType> getContentTypes() {
      return cmaClient.contentTypes().fetchAll(configuration.contentful.spaceId);
  }

}
