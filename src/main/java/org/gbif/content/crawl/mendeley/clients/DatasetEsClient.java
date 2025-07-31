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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetEsClient {

  @Data
  @Builder
  public static class DatasetSearchResponse {

    private final String key;
    private final String projectIdentifier;
    private final String programmeAcronym;

  }

  private static final int PAGE_SIZE = 500;

  private final ContentCrawlConfiguration configuration;

  private final ElasticsearchClient esClient;

  private final ContentEsClient contentEsClient;
  private final Cache<String, DatasetSearchResponse> cache;

  public DatasetEsClient(@NonNull ContentCrawlConfiguration configuration) {
    this.configuration = configuration;
    this.esClient = ElasticSearchUtils.buildEsClient(configuration.getMendeley().getDatasetElasticSearch());
    cache = new Cache2kBuilder<String, DatasetSearchResponse>(){}
      .eternal(true)
      .entryCapacity(20_000)
      .permitNullValues(true)
      .build();
    this.contentEsClient = new ContentEsClient(configuration);
  }

  private static String getProjectIdentifier(Hit<Object> searchHit) {
    Map<String, Object> source = (Map<String, Object>) searchHit.source();
    if (source.containsKey("project")) {
      HashMap<String,?> project = (HashMap<String,?>)source.get("project");
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

  private DatasetSearchResponse toDatasetSearchResponse(Hit<Object> searchHit) {
    String projectIdentifier = getProjectIdentifier(searchHit);
    return DatasetSearchResponse.builder()
            .key(searchHit.id())
            .projectIdentifier(projectIdentifier)
            .programmeAcronym(getProgrammeAcronym(projectIdentifier))
            .build();
  }

  public Optional<DatasetSearchResponse> get(String datasetKey) {
    return Optional.ofNullable(cache.get(datasetKey));
  }

  @SneakyThrows
  private DatasetSearchResponse getFromElastic(String datasetKey) {
    SearchRequest searchRequest = new SearchRequest.Builder()
      .index(configuration.getMendeley().getDatasetIndex())
      .query(q -> q.ids(ids -> ids.values(datasetKey)))
      .size(1)
      .source(s -> s.filter(f -> f.includes("project.identifier")))
      .build();
    
    SearchResponse<Object> searchResponse = esClient.search(searchRequest, Object.class);
    if (searchResponse.hits().total().value() > 0) {
      return toDatasetSearchResponse(searchResponse.hits().hits().get(0));
    }
    return null;
  }

  @SneakyThrows
  public void loadAllWithProjectIds() {
    SearchRequest.Builder searchRequestBuilder = new SearchRequest.Builder()
      .index(configuration.getMendeley().getDatasetIndex())
      .query(q -> q.exists(e -> e.field("project.identifier")))
      .size(PAGE_SIZE)
      .source(s -> s.filter(f -> f.includes("project.identifier")));

    SearchResponse<Object> searchResponse = esClient.search(searchRequestBuilder.build(), Object.class);
    int from = 0;
    
    while (searchResponse.hits().hits().size() > 0) {
      log.info("Loading {} datasets from {} into the cache", PAGE_SIZE, from);
      searchResponse.hits().hits().forEach(searchHit -> cache.put(searchHit.id(), toDatasetSearchResponse(searchHit)));
      from += searchResponse.hits().hits().size();
      
      searchRequestBuilder.from(from);
      searchResponse = esClient.search(searchRequestBuilder.build(), Object.class);
    }

    log.info("Dataset cache built with {} entries", cache.keys().size());
  }

}
