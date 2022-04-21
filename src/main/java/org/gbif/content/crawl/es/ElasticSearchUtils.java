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

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;

/**
 * Common ElasticSearch utility methods.
 */
public class ElasticSearchUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchUtils.class);

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  //Index settings used at indexing time
  private static final Settings INDEXING_SETTINGS = Settings.builder()
                                                      .put("index.refresh_interval", "-1")
                                                      .put("index.number_of_shards", "1")
                                                      .put("index.number_of_replicas", "0")
                                                      .put("index.translog.durability","async")
                                                      .build();

  //Index settings used at production/searching time
  private static final Settings SEARCH_SETTINGS = Settings.builder()
                                                    .put("index.refresh_interval", "1s")
                                                    .put("index.number_of_replicas", "0")
                                                    .build();

  //This an alias used for all active cms/content indices
  private static final String CONTENT_ALIAS = "content";

  /**
   * Utility class must have private methods.
   */
  private ElasticSearchUtils() {
    //NOP
  }

  /** Creates ElasticSearch client using default connection settings. */
  public static RestHighLevelClient buildEsClient(ContentCrawlConfiguration.ElasticSearch esClientConfiguration) {
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
    PoolingNHttpClientConnectionManager r;

    return new RestHighLevelClient(
      RestClient.builder(hosts)
        .setRequestConfigCallback(
          requestConfigBuilder ->
            requestConfigBuilder
              .setConnectTimeout(esClientConfiguration.getConnectionTimeOut())
              .setSocketTimeout(esClientConfiguration.getSocketTimeOut())
              .setConnectionRequestTimeout(
                esClientConfiguration.getConnectionRequestTimeOut()))
        .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS));
  }


  /**
   * Creates, if doesn't exists, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  public static void createIndex(RestHighLevelClient esClient,
                                 String idxName, String source) {
    try {
      LOG.info("Index into Elasticsearch Index {} ", idxName);
      //create ES idx if it doesn't exist
      if (esClient.indices().exists(new GetIndexRequest(idxName), RequestOptions.DEFAULT)) {
        esClient.indices().delete(new DeleteIndexRequest(idxName), RequestOptions.DEFAULT);
      }
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(idxName)
                                                .mapping(source, XContentType.JSON)
                                                .settings(INDEXING_SETTINGS);
      esClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    } catch (IOException ex) {
      LOG.error("Error creating index", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Creates, if doesn't exists, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  public static void createIndex(RestHighLevelClient esClient, ContentCrawlConfiguration.IndexBuild configuration,
                                 String source) {
    createIndex(esClient, getEsIndexingIdxName(configuration.getEsIndexName()), source);
  }

  /**
   * This method delete all the indexes associated to the alias and associates the alias to toIdx.
   */
  public static void swapIndexToAlias(RestHighLevelClient esClient, String alias, String toIdx) {
    try {
      //Update setting to search production
      esClient.indices().putSettings(new UpdateSettingsRequest().indices(toIdx).settings(SEARCH_SETTINGS), RequestOptions.DEFAULT);

      //Keeping 1 segment per idx should be enough for small indexes
      esClient.indices().forcemerge(new ForceMergeRequest(toIdx).maxNumSegments(1), RequestOptions.DEFAULT);

      //Sets the idx alias
      GetAliasesResponse aliasesGetResponse = esClient.indices()
                                            .getAlias(new GetAliasesRequest().aliases(alias), RequestOptions.DEFAULT);

      IndicesAliasesRequest swapAliasesRequest = new IndicesAliasesRequest();
      swapAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                                          .index(toIdx)
                                          .aliases(alias, CONTENT_ALIAS));


      //add the removal all existing indexes of that alias
      aliasesGetResponse.getAliases()
        .keySet().forEach(idx -> swapAliasesRequest
                                  .addAliasAction( new IndicesAliasesRequest
                                                    .AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE_INDEX)
                                                    .index(idx)));

      //Execute all the alias operations in a single/atomic call
      esClient.indices().updateAliases(swapAliasesRequest, RequestOptions.DEFAULT);

    } catch (IOException ex) {
      throw new IllegalStateException(ex);
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
