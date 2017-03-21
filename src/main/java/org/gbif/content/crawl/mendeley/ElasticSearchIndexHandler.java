package org.gbif.content.crawl.mendeley;

import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.es.ElasticSearchUtils.buildEsClient;
import static org.gbif.content.crawl.es.ElasticSearchUtils.createIndex;

/**
 * Parses the documents from the response and adds them to the index.
 */
public class ElasticSearchIndexHandler implements ResponseHandler {

  //Mendeley fields used by this handler
  private static final String ML_ID_FL = "id";
  private static final String ML_TAGS_FL = "tags";

  //Elasticsearch fields created by this handler
  private static final String ES_AUTHORS_COUNTRY_FL = "authors_country";
  private static final String ES_BIODIVERSITY_COUNTRY_FL = "biodiversity_country";
  private static final String ES_GBIF_REGION_FL = "gbif_region";

  private static final String ES_MAPPING_FILE = "mendeley_mapping.json";

  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchIndexHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Client client;
  private final ContentCrawlConfiguration conf;

  public ElasticSearchIndexHandler(ContentCrawlConfiguration conf) {
    this.conf = conf;
    LOG.info("Connecting to ES cluster {}:{}", conf.elasticSearch.host, conf.elasticSearch.port);
    client = buildEsClient(conf.elasticSearch);
    createIndex(client, conf.mendeley.indexBuild, ES_MAPPING_FILE);
  }

  /**
   * Bulk loads the response as JSON into ES.
   * @param responseAsJson To load.
   */
  @Override
  public void handleResponse(String responseAsJson) throws IOException {
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    //process each Json node
    MAPPER.readTree(responseAsJson).elements().forEachRemaining(document -> {
      if (document.has(ML_TAGS_FL)) {
        handleTags(document);
      }
      bulkRequest.add(client.prepareIndex(conf.mendeley.indexBuild.esIndexName, conf.mendeley.indexBuild.esIndexType,
                                          document.get(ML_ID_FL).asText()).setSource(document.toString()));
    });

    BulkResponse bulkResponse = bulkRequest.get();
    if (bulkResponse.hasFailures()) {
      LOG.error("Error indexing.  First error message: {}", bulkResponse.getItems()[0].getFailureMessage());
    } else {
      LOG.info("Indexed [{}] documents", bulkResponse.getItems().length);
    }
  }

  /**
   * Process tags. Adds publishers countries and biodiversity countries from tag values.
   */
  private static void handleTags(JsonNode document) {
    ArrayNode publishersCountries = MAPPER.createArrayNode();
    ArrayNode biodiversityCountries = MAPPER.createArrayNode();
    ArrayNode regions = MAPPER.createArrayNode();
    document.get(ML_TAGS_FL).elements().forEachRemaining(node -> {
      String value = node.textValue();
      Optional.ofNullable(Country.fromIsoCode(value))
        .ifPresent(country -> publishersCountries.add(country.getIso2LetterCode()));

      //VocabularyUtils uses Guava optionals
      com.google.common.base.Optional<Country> bioCountry = VocabularyUtils.lookup(value, Country.class);
      if (bioCountry.isPresent()) {
        biodiversityCountries.add(value);
        Optional.ofNullable(bioCountry.get().getGbifRegion()).ifPresent(region -> regions.add(region.name()));
      }
    });
    ((ObjectNode)document).putArray(ES_AUTHORS_COUNTRY_FL).addAll(publishersCountries);
    ((ObjectNode)document).putArray(ES_BIODIVERSITY_COUNTRY_FL).addAll(biodiversityCountries);
    ((ObjectNode)document).putArray(ES_GBIF_REGION_FL).addAll(regions);
  }

  @Override
  public void finish() {
    client.close();
  }
}
