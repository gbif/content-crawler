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

public class ContentEsClient {

  @Data
  @Builder
  public static class ProjectResponse {

    private final String identifier;
    private final String programmeAcronym;

  }

  private final RestHighLevelClient esClient;

  private final Cache<String, Optional<ProjectResponse>> cache;

  public ContentEsClient(@NonNull ContentCrawlConfiguration configuration) {
    this.esClient = ElasticSearchUtils.buildEsClient(configuration.getElasticSearch());
    cache = new Cache2kBuilder<String, Optional<ProjectResponse>>(){}
      .loader(this::getFromElastic)
      .eternal(true)
      .build();
  }

  @SneakyThrows
  private String getProgrammeAcronym(String programmeId) {
    if (programmeId != null) {
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        .size(1)
        .fetchSource(new String[]{"acronym"}, null)
        .query(QueryBuilders.termQuery("_id", programmeId));
      SearchRequest searchRequest = new SearchRequest();
      searchRequest.indices("programme");
      searchRequest.source(searchSourceBuilder);
      SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
      if (searchResponse.getHits().getTotalHits().value > 0) {
        Object acronym = searchResponse.getHits().getAt(0).getSourceAsMap().get("acronym");
        if(acronym != null) {
          return acronym.toString();
        }
    }
    }
    return null;
  }

  private static String getProgrammeId(SearchHit searchHit) {
    if (searchHit.getSourceAsMap().containsKey("programme")) {
      HashMap<String,?> project = (HashMap<String,?>)searchHit.getSourceAsMap().get("programme");
      Object identifier = project.get("id");
      if (identifier != null) {
        return (String)identifier;
      }
    }
    return null;
  }

  private ProjectResponse toProjectResponse(SearchHit searchHit) {
    return ProjectResponse.builder()
            .identifier(searchHit.getId())
            .programmeAcronym(getProgrammeAcronym(getProgrammeId(searchHit)))
            .build();
  }

  public Optional<ProjectResponse> get(String projectId) {
    return cache.get(projectId);
  }

  @SneakyThrows
  private Optional<ProjectResponse> getFromElastic(String projectId) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                                                .size(1)
                                                .fetchSource(new String[]{"programme.id"}, null)
                                                .query(QueryBuilders.termQuery("_id", projectId));
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices("project");
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
    if (searchResponse.getHits().getTotalHits().value > 0) {
      return Optional.of(toProjectResponse(searchResponse.getHits().getAt(0)));
    }
    return Optional.empty();
  }
}
