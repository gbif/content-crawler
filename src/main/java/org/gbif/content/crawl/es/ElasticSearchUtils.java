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
package org.gbif.content.crawl.es;

import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.indices.Translog;
import co.elastic.clients.elasticsearch.indices.TranslogDurability;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;

/**
 * Common ElasticSearch utility methods.
 */
public class ElasticSearchUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchUtils.class);

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  //This an alias used for all active cms/content indices
  private static final String CONTENT_ALIAS = "content";

  /**
   * Utility class must have private methods.
   */
  private ElasticSearchUtils() {
    //NOP
  }

  /** Creates ElasticSearch client using default connection settings. */
  public static ElasticsearchClient buildEsClient(ContentCrawlConfiguration.ElasticSearch esClientConfiguration) {
    String[] hostsUrl = esClientConfiguration.getHost().split(",");
    HttpHost[] hosts = new HttpHost[hostsUrl.length];
    int i = 0;
    for (String host : hostsUrl) {
      try {
        URL url = new URL(host);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    RestClientBuilder builder = RestClient.builder(hosts)
        .setRequestConfigCallback(
          requestConfigBuilder ->
            requestConfigBuilder
              .setConnectTimeout(esClientConfiguration.getConnectionTimeOut())
              .setSocketTimeout(esClientConfiguration.getSocketTimeOut())
              .setConnectionRequestTimeout(
                esClientConfiguration.getConnectionRequestTimeOut()));

    RestClient restClient = builder.build();
    RestClientTransport transport = new RestClientTransport(restClient, new co.elastic.clients.json.jackson.JacksonJsonpMapper());
    
    return new ElasticsearchClient(transport);
  }

  /**
   * Creates, if it doesn't exist, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  public static void createIndex(ElasticsearchClient esClient,
                                 String idxName, String source) {
    try {
      LOG.info("Index into Elasticsearch Index {} ", idxName);
      //create ES idx if it doesn't exist
      if (esClient.indices().exists(new ExistsRequest.Builder().index(idxName).build()).value()) {
        esClient.indices().delete(new DeleteIndexRequest.Builder().index(idxName).build());
      }
      
      CreateIndexRequest.Builder createIndexRequestBuilder = new CreateIndexRequest.Builder()
          .index(idxName)
          .mappings(TypeMapping.of(m -> m.withJson(new java.io.StringReader(source))))
          .settings(s -> s
              .refreshInterval(Time.of(t -> t.time("-1")))
              .numberOfShards("1")
              .numberOfReplicas("0")
              .translog(Translog.of(t -> t.durability(TranslogDurability.Async))));
      
      esClient.indices().create(createIndexRequestBuilder.build());
    } catch (IOException ex) {
      LOG.error("Error creating index", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Creates, if it doesn't exist, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  public static void createIndex(ElasticsearchClient esClient, ContentCrawlConfiguration.IndexBuild configuration,
                                 String source) {
    createIndex(esClient, getEsIndexingIdxName(configuration.getEsIndexName()), source);
  }

  /**
   * This method removes all indexes associated with the alias and associates the alias to toIdx.
   */
  public static void swapIndexToAlias(
      ElasticsearchClient esClient,
      String alias,
      String toIdx,
      ContentCrawlConfiguration.IndexBuild indexConfig
  ) {
    try {
      LOG.info("Swapping alias '{}' to point to index '{}'", alias, toIdx);

      // Step 1: Apply search-specific settings to the target index
      esClient.indices().putSettings(ps -> ps
          .index(toIdx)
          .settings(s -> s
              .refreshInterval(Time.of(t -> t.time("1s")))
              .numberOfReplicas("0")
              .maxResultWindow(indexConfig.getMaxResultWindow())
          )
      );

      // Step 2: Force merge to 1 segment per index for small indexes
      esClient.indices().forcemerge(b -> b
          .index(toIdx)
          .maxNumSegments(1L)
      );

      // Step 3: Add aliases to the new index
      List<Action> actions = new ArrayList<>();
      
      // Add content type alias with write index designation
      actions.add(Action.of(a -> a
          .add(add -> add.index(toIdx).alias(alias).isWriteIndex(true))
      ));

      // Add content alias
      actions.add(Action.of(a -> a
          .add(add -> add.index(toIdx).alias(CONTENT_ALIAS).isWriteIndex(false))
      ));

      // Step 4: Execute all alias operations in a single atomic call
      esClient.indices().updateAliases(ua -> ua.actions(actions));

      LOG.info("Successfully added index '{}' to alias '{}' and content alias", toIdx, alias);
    } catch (Exception ex) {
      LOG.error("Failed to add index '{}' to aliases: {}", toIdx, ex.getMessage());
      throw new IllegalStateException("Failed to add aliases", ex);
    }
  }

  /**
   * Reads the content of ES_MAPPINGS_FILE into a String.
   */
  public static String indexMappings(String mappingsFileName) {
    try {
      return new String(IOUtils.toByteArray(Thread.currentThread().getContextClassLoader()
                                              .getResourceAsStream(mappingsFileName)), StandardCharsets.UTF_8);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Gets the ElasticSearch index name.
   */
  public static String getEsIdxName(String contentTypeName) {
    return REPLACEMENTS.matcher(contentTypeName).replaceAll("").toLowerCase();
  }

  /**
   * Index name to be used while indexing.
   * @return getEsIdxName(contentTypeName) + time in milliseconds
   */
  public static String getEsIndexingIdxName(String contentTypeName) {
    return getEsIdxName(contentTypeName) + new Date().getTime();
  }

  /**
   * Translates a sentence type text into upper camel case format.
   * For example: "Hola Morten" will be transformed into "holaMorten".
   */
  public static String toFieldNameFormat(CharSequence sentence) {
    return CaseFormat.UPPER_UNDERSCORE
      .to(CaseFormat.LOWER_CAMEL, REPLACEMENTS.matcher(sentence).replaceAll("_").toUpperCase());
  }

}
