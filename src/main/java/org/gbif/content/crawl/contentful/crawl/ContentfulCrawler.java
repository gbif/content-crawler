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

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.contentful.java.cda.CDAClient;
import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAContentType;
import com.google.common.base.Preconditions;

import static org.gbif.content.crawl.es.ElasticSearchUtils.buildEsClient;


/**
 * Pulls content from Contentful and stores it in ElasticSearch indexes.
 */
public class ContentfulCrawler {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulCrawler.class);

  //3 Minutes
  private static final int CONNECTION_TO =  3;

  private final ContentCrawlConfiguration.Contentful configuration;
  private final ContentCrawlConfiguration.ElasticSearch esConfiguration;
  private final CDAClient cdaClient;
  private final CMAClient cmaClient;
  private final RestHighLevelClient esClient;

  private String newsContentTypeId;
  private String articleContentTypeId;

  /**
   * ElasticSearch and Contentful configuration are required to create an instance of this class.
   */
  public ContentfulCrawler(ContentCrawlConfiguration configuration) {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.getElasticSearch(), "ElasticSearch configuration can't be null");
    Preconditions.checkNotNull(configuration.getContentful(), "Contentful configuration can't be null");
    this.configuration = configuration.getContentful();
    cdaClient = buildCdaClient();
    cmaClient = buildCmaClient();
    esClient = buildEsClient(configuration.getElasticSearch());
    esConfiguration = configuration.getElasticSearch();
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

    //Gets the ContentType.resourceId for news and articles, which vary by space
    newsContentTypeId = getContentTypeId(webContentTypes, configuration.getNewsContentType());
    articleContentTypeId = getContentTypeId(webContentTypes, configuration.getArticleContentType());


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
        if (contentType.getName().equals(configuration.getCountryVocabulary())) {
          vocabularyTerms.loadCountryVocabulary(contentType);
        } else {
          //Loads vocabulary into memory
          vocabularyTerms.loadVocabulary(contentType);
        }
      });
    return vocabularyTerms;
  }

  /**
   * Gets the ID of the named content type.
   */
  private String getContentTypeId(Collection<CMAContentType> contentTypes, String name) {
    return contentTypes.stream()
      .filter(contentType -> contentType.getName().equals(name))
      .findFirst()
      .orElseThrow(() -> new IllegalStateException("ContentType not found for [" + name + "]")).getId();
  }

  /**
   * Crawls a list of ContentTypes into ElasticSearch.
   */
  private void crawlContentTypes(Collection<CMAContentType> contentTypes, MappingGenerator mappingGenerator,
                                 VocabularyTerms vocabularyTerms) {
    //The stream is sorted to ensure that the News and Article content types are crawled first, so they exist and may
    // be updated to store reverse links into it
    contentTypes.stream()
      .filter(contentType -> configuration.getContentTypes().contains(contentType.getName()))
      .sorted(Comparator.comparingInt(ct -> configuration.getContentTypes().indexOf(ct.getName())))
      .forEach(contentType -> {
        ContentTypeCrawler contentTypeCrawler = new ContentTypeCrawler(contentType,
                                                                       mappingGenerator,
                                                                       esClient,
                                                                       cdaClient,
                                                                       vocabularyTerms,
                                                                       newsContentTypeId,
                                                                       articleContentTypeId,
                                                                       configuration.getIndexBuild());
        contentTypeCrawler.crawl();
      });
  }


  /**
   * @return a new instance of a Contentful CDAClient.
   */
  private CDAClient buildCdaClient() {
    CDAClient.Builder builder = CDAClient.builder();
    return builder
            .setSpace(configuration.getSpaceId())
            .setToken(configuration.getCdaToken())
            .setEnvironment(configuration.getEnvironmentId())
            .setCallFactory(builder.defaultCallFactoryBuilder().readTimeout(CONNECTION_TO, TimeUnit.MINUTES).retryOnConnectionFailure(true).build()).build();
  }

  /**
   * @return a new instance of a Contentful CDAClient.
   */
  private CMAClient buildCmaClient() {
    return new CMAClient.Builder()
                .setSpaceId(configuration.getSpaceId())
                .setEnvironmentId(configuration.getEnvironmentId())
                .setAccessToken(configuration.getCmaToken()).build();
  }


  /**
   * @return a partition of ContentTypes split into sets:  TRUE: vocabularies and FALSE: non-vocabularies/web content.
   */
  private Map<Boolean,List<CMAContentType>> getContentTypes() {
    Collection<String> allContentTypes = new LinkedHashSet<>(configuration.getVocabularies());
    allContentTypes.addAll(configuration.getContentTypes());
    allContentTypes.add(configuration.getNewsContentType());
    return cmaClient.contentTypes().fetchAll(configuration.getSpaceId(), configuration.getEnvironmentId())
            .getItems().stream().filter(contentType -> allContentTypes.contains(contentType.getName()))
            .collect(Collectors.partitioningBy(contentType -> configuration.getVocabularies()
                                                                .contains(contentType.getName())));
  }

}
