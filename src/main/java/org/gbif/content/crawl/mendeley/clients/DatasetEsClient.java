package org.gbif.content.crawl.mendeley.clients;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;
import org.gbif.content.crawl.es.ElasticSearchUtils;

import java.util.HashMap;
import java.util.Optional;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class DatasetEsClient {

  @Data
  @Builder
  public static class DatasetSearchResponse {

    private final String key;
    private final String projectIdentifier;

  }

  private final ContentCrawlConfiguration configuration;

  private final RestHighLevelClient esClient;

  private final Cache<String, Optional<DatasetSearchResponse>> cache;

  public DatasetEsClient(@NonNull ContentCrawlConfiguration configuration) {
    this.configuration = configuration;
    this.esClient = ElasticSearchUtils.buildEsClient(configuration.getMendeley().getDatasetElasticSearch());
    cache = new Cache2kBuilder<String, Optional<DatasetSearchResponse>>(){}
      .loader(this::getFromElastic)
      .eternal(true)
      .build();
  }

  private static String getProjectIdentifier(SearchHit searchHit) {
    if (searchHit.getSourceAsMap().containsKey("project")) {
      HashMap<String,?> project = (HashMap<String,?>)searchHit.getSourceAsMap().get("project");
      Object identifier = project.get("identifier");
      if(identifier != null) {
        return (String)identifier;
      }
    }
    return null;
  }

  private static DatasetSearchResponse toDatasetSearchResponse(SearchHit searchHit) {
    return DatasetSearchResponse.builder()
            .key(searchHit.getId())
            .projectIdentifier(getProjectIdentifier(searchHit))
            .build();
  }

  public Optional<DatasetSearchResponse> get(String datasetKey) {
    return cache.get(datasetKey);
  }

  @SneakyThrows
  private Optional<DatasetSearchResponse> getFromElastic(String datasetKey) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                                                .size(1)
                                                .fetchSource(new String[]{"project.identifier"}, null)
                                                .query(QueryBuilders.termQuery("_id", datasetKey));
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(configuration.getMendeley().getDatasetIndex());
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
    if (searchResponse.getHits().getTotalHits().value > 0) {
      return Optional.of(toDatasetSearchResponse(searchResponse.getHits().getAt(0)));
    }
    return Optional.empty();
  }
}
