package org.gbif.content.crawl.es;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Common ElasticSearch utility methods.
 */
public class ElasticSearchUtils {

  /**
   * Utility class must have private methods.
   */
  private ElasticSearchUtils() {
    //NOP
  }


  /**
   * Creates a new instance of a ElasticSearch client.
   */
  public static Client buildEsClient(ContentCrawlConfiguration.ElasticSearch configuration) {
    try {
      Settings settings = Settings.builder().put("cluster.name", configuration.cluster).build();
      return new PreBuiltTransportClient(settings).addTransportAddress(
        new InetSocketTransportAddress(InetAddress.getByName(configuration.host),
                                       configuration.port));
    } catch (UnknownHostException ex) {
      throw new IllegalStateException(ex);
    }
  }


  /**
   * Creates, if doesn't exists, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  public static void createIndex(Client esClient, ContentCrawlConfiguration.IndexBuild configuration,
                                 String idxName, String source) {
    //create ES idx if it doesn't exists
    if (!esClient.admin().indices().prepareExists(idxName).get().isExists()) {
      esClient.admin().indices().prepareCreate(idxName)
        .addMapping(configuration.esIndexType, source).get();
    } else if (configuration.deleteIndex) { //if the index exists and should be recreated
      //Delete the index
      esClient.admin().indices().prepareDelete(idxName).get();
      //Re-create the index
      esClient.admin().indices().prepareCreate(idxName)
        .addMapping(configuration.esIndexType, source).get();
    }
  }


  /**
   * Creates, if doesn't exists, an ElasticSearch index that matches the name of the contentType.
   * If the flag configuration.contentful.deleteIndex is ON and the index exist, it will be removed.
   */
  public static void createIndex(Client esClient, ContentCrawlConfiguration.IndexBuild configuration,
                                 String source) {
    createIndex(esClient, configuration, configuration.esIndexName, source);
  }





  /**
   * Reads the content of ES_MAPPINGS_FILE into a String.
   */
  public static String indexMappings(String mappingsFileName) {
    try {
      return new String(IOUtils.toByteArray(Thread.currentThread().getContextClassLoader()
                                              .getResourceAsStream(mappingsFileName)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

}
