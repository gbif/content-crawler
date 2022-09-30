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
package org.gbif.content.crawl.mendeley.clients;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;
import org.gbif.content.crawl.es.ElasticSearchUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetEsClient implements Closeable {

  @Data
  @Builder
  public static class DatasetSearchResponse {

    private final String key;
    private final String projectIdentifier;
    private final String programmeAcronym;

  }

  private static final int PAGE_SIZE = 500;

  private final ContentCrawlConfiguration configuration;

  private final RestHighLevelClient esClient;

  private final ContentEsClient contentEsClient;
  private final Cache<String, DatasetSearchResponse> cache;

  public DatasetEsClient(@NonNull ContentCrawlConfiguration configuration) {
    this.configuration = configuration;
    this.esClient = ElasticSearchUtils.buildEsClient(configuration.getMendeley().getDatasetElasticSearch());
    cache = new Cache2kBuilder<String, DatasetSearchResponse>(){}
      .eternal(true)
      .entryCapacity(20_000)
      .loader(this::getFromElastic)
      .permitNullValues(true)
      .build();
    this.contentEsClient = new ContentEsClient(configuration);
  }

  private static String getProjectIdentifier(SearchHit searchHit) {
    if (searchHit.getSourceAsMap().containsKey("project")) {
      HashMap<String,?> project = (HashMap<String,?>)searchHit.getSourceAsMap().get("project");
      Object identifier = project.get("identifier");
      if (identifier != null) {
        return (String)identifier;
      }
    }
    return null;
  }

  private Optional<ContentEsClient.ProjectResponse> getProjectData(String projectId) {
    return contentEsClient.get(projectId);
  }

  private String getProgrammeAcronym(String projectId) {
    if (projectId != null) {
        return getProjectData(projectId)
          .map(ContentEsClient.ProjectResponse::getProgrammeAcronym)
          .orElse(null);
    }
    return null;
  }

  private DatasetSearchResponse toDatasetSearchResponse(SearchHit searchHit) {
    String projectIdentifier = getProjectIdentifier(searchHit);
    return DatasetSearchResponse.builder()
            .key(searchHit.getId())
            .projectIdentifier(projectIdentifier)
            .programmeAcronym(getProgrammeAcronym(projectIdentifier))
            .build();
  }

  public Optional<DatasetSearchResponse> get(String datasetKey) {
    return Optional.ofNullable(cache.get(datasetKey));
  }

  @SneakyThrows
  private DatasetSearchResponse getFromElastic(String datasetKey) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                                                .size(1)
                                                .fetchSource(new String[]{"project.identifier"}, null)
                                                .query(QueryBuilders.idsQuery().addIds(datasetKey));
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(configuration.getMendeley().getDatasetIndex());
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
    if (searchResponse.getHits().getTotalHits().value > 0) {
      return toDatasetSearchResponse(searchResponse.getHits().getAt(0));
    }
    return null;
  }

  @SneakyThrows
  public void loadAllWithProjectIds() {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
      .size(PAGE_SIZE)
      .from(0)
      .fetchSource(new String[]{"project.identifier"}, null)
      .query(QueryBuilders.existsQuery("project.identifier"));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(configuration.getMendeley().getDatasetIndex());
    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
    while (searchResponse.getHits().getHits().length > 0) {
      log.info("Loading {} datasets from {} into the cache", searchSourceBuilder.size(), searchSourceBuilder.from());
      searchResponse.getHits().iterator().forEachRemaining(searchHit -> cache.put(searchHit.getId(), toDatasetSearchResponse(searchHit)));
      searchSourceBuilder.from(searchSourceBuilder.from() + searchResponse.getHits().getHits().length);
      searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    log.info("Dataset cache built with {} entries", cache.keys().size());
  }

  @Override
  public void close() throws IOException {
    esClient.close();
    contentEsClient.close();
  }

}
