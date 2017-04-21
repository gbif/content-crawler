package org.gbif.content.crawl.contentful;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;
import org.gbif.content.crawl.contentful.meta.Meta;
import org.gbif.content.crawl.es.ElasticSearchUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAContentType;
import com.google.common.base.CaseFormat;
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

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  private final ContentCrawlConfiguration configuration;
  private final CDAClient cdaClient;
  private final Client esClient;

  //Country Content Type Id
  private String countryContentTypeId;

  //News Content Type Id
  private String newsContentTypeId;

  /**
   * vocabularyName -> { contentId -> defaultValue} }
   */
  private final Set<String> vocabulariesContentTypeIds;

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
    vocabulariesContentTypeIds = new HashSet<>();
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
   * Executes the Crawler in a synchronous way, i.e.: process each resource per content type sequentially.
   */
  public void run() {
    List<CDAContentType> contentTypes = getContentTypes().items().stream()
                                          .map(cdaResource -> (CDAContentType)cdaResource).collect(Collectors.toList());
    Set<CDAContentType> vocContentTypes = contentTypes.stream()
      .filter(contentType -> configuration.contentful.vocabularies.contains(contentType.name()))
      .collect(Collectors.toSet());
    vocContentTypes.forEach(contentType -> {
      //Keeps the country vocabulary ID for future use
      if (contentType.name().equals(configuration.contentful.countryVocabulary)) {
        countryContentTypeId = contentType.id();
      }
      //Loads vocabulary into memory
      vocabulariesContentTypeIds.add(contentType.id());
    });

    //Mapping generator can be re-used for all content types
    MappingGenerator mappingGenerator = new MappingGenerator(vocContentTypes);

    contentTypes.stream().filter(contentType -> configuration.contentful.contentTypes.contains(contentType.name()))
      .forEach(contentType -> {
        if (contentType.name().equals(configuration.contentful.newsContentType)) {
          newsContentTypeId = contentType.id();
        }
        ContentTypeCrawler contentTypeCrawler = new ContentTypeCrawler(contentType, mappingGenerator, esClient,
                                                                       configuration, cdaClient, vocabulariesContentTypeIds,
                                                                       countryContentTypeId, newsContentTypeId);
        contentTypeCrawler.crawl();
      });
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
