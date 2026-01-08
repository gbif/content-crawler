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

import java.util.Arrays;
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

/**
 * Elasticsearch client to get information from project and programme indices.
 */
public class ContentEsClient {

  /**
   * Data obtained from Elasticsearch.
   */
  @Data
  @Builder
  public static class ProjectResponse {

    private final String identifier;
    private final String programmeAcronym;

  }

  private final ElasticsearchClient esClient;

  private final Cache<String, Optional<ProjectResponse>> cache;

  public ContentEsClient(@NonNull ContentCrawlConfiguration configuration) {
    this.esClient = ElasticSearchUtils.buildEsClient(configuration.getElasticSearch());
    //Cache2k with loader
    cache = new Cache2kBuilder<String, Optional<ProjectResponse>>(){}
      .loader(this::getFromElastic)
      .eternal(true)
      .build();
  }


  private static SearchRequest buildSearchByIdsRequest(String index, String[] fetchFields,String id) {
    return new SearchRequest.Builder()
        .size(1) //only one result is expected
        .source(s -> s.filter(f -> f.includes(Arrays.asList(fetchFields))))
        .query(q -> q.ids(ids -> ids.values(id)))
        .index(index)
        .build();
  }

  /**
   * Queries the programme index to get the programme acronym.
   */
  @SneakyThrows
  private String getProgrammeAcronym(String programmeId) {
    if (programmeId != null) {
      SearchRequest searchRequest = buildSearchByIdsRequest("programme", new String[]{"acronym"}, programmeId);

      SearchResponse<Object> searchResponse = esClient.search(searchRequest, Object.class);

      if (searchResponse.hits().total().value() > 0) { //are there results?
        Map<String, Object> source = (Map<String, Object>) searchResponse.hits().hits().get(0).source();
        Object acronym = source.get("acronym");
        if (acronym != null) {// hasAcronym
          return acronym.toString();
        }
      }
    }
    return null;
  }

  /**
   * Extracts the programme.id from the response.
   */
  private static String getProgrammeId(Hit<Object> searchHit) {
    Map<String, Object> source = (Map<String, Object>) searchHit.source();
    if (source.containsKey("programme")) {
      HashMap<String,?> project = (HashMap<String,?>)source.get("programme");
      Object identifier = project.get("id");
      if (identifier != null) {
        return (String)identifier;
      }
    }
    return null;
  }

  /**
   * Converts the hit result to a ProjectResponse.
   */
  private ProjectResponse toProjectResponse(Hit<Object> searchHit) {
    return ProjectResponse.builder()
            .identifier(searchHit.id())
            .programmeAcronym(getProgrammeAcronym(getProgrammeId(searchHit)))
            .build();
  }

  /**
   * Gets or loads the result from the cache.
   */
  public Optional<ProjectResponse> get(String projectId) {
    return cache.get(projectId);
  }

  /**
   * Tries to load a result form Elasticsearch.
   */
  @SneakyThrows
  private Optional<ProjectResponse> getFromElastic(String projectId) {
    SearchRequest searchRequest = buildSearchByIdsRequest("project", new String[]{"programme.id"}, projectId);

    SearchResponse<Object> searchResponse = esClient.search(searchRequest, Object.class);
    if (searchResponse.hits().total().value() > 0) {
      return Optional.of(toProjectResponse(searchResponse.hits().hits().get(0)));
    }
    return Optional.empty();
  }

}
