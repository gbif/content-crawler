package org.gbif.content.crawl.mendeley;

import org.gbif.api.model.registry.Citation;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.sql.ConnectionEvent;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;

public class DatasetsetUsagesExport {

  private static final String DATASETS_DOWNLOADS_QUERY = "SELECT od.doi, doc.dataset_key, d.publishing_organization_key, doc.download_key "
                                                         + "FROM dataset_occurrence_download doc "
                                                         + "JOIN dataset d ON d.key = doc.dataset_key "
                                                         + "JOIN occurrence_download od ON od.doi = ? "
                                                         + "WHERE doc.download_key = od.key "
                                                         + "UNION "
                                                         + "SELECT d.doi, d.key AS dataset_key, d.publishing_organization_key, NULL as download_key "
                                                         + "FROM dataset d "
                                                         + "WHERE d.doi = ?";

  private final Cache<String, Collection<CrawlPipeline.DatasetCitation>> cache;

  private final DataSource dataSource;

  public DatasetsetUsagesExport(Properties configuration) {
    dataSource = getDataSource(configuration);
    cache = new Cache2kBuilder<String,Collection<CrawlPipeline.DatasetCitation>>(){}
              .loader(new CacheLoader<String, Collection<CrawlPipeline.DatasetCitation>>() {
                @Override
                public Collection<CrawlPipeline.DatasetCitation> load(final String key) throws Exception {
                  return loadCitations(key);
                }
              }).build();
  }



  private DataSource getDataSource(Properties properties)
  {
    HikariConfig config = new HikariConfig(properties);
    return new HikariDataSource(config);
  }


  private Collection<CrawlPipeline.DatasetCitation> loadCitations(String doi) {
    Collection<CrawlPipeline.DatasetCitation> citations = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(DATASETS_DOWNLOADS_QUERY)) {
      preparedStatement.setFetchSize(18000);
      preparedStatement.setString(1, doi);
      preparedStatement.setString(2, doi);

      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()){
        citations.add(new CrawlPipeline.DatasetCitation(resultSet.getString("dataset_key"),
                                                        resultSet.getString("publishing_organization_key"),
                                                        resultSet.getString("download_key")));
      }
      return citations;
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }


  public Collection<CrawlPipeline.DatasetCitation> getCitations(String doi) {
    return cache.get(doi);
  }


  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
    properties.put("dataSource.serverName", "pg1.gbif.org");
    properties.put("dataSource.databaseName", "prod_b_registry");
    properties.put("dataSource.user", "registry");
    properties.put("dataSource.password","h56jebHD6h");
    properties.put("maximumPoolSize","12");
    properties.put("connectionTimeout","3000");
    properties.put("minimumIdle","2");
    properties.put("idleTimeout","60000");
    properties.put("connectionInitSql","SET work_mem='64MB'");
    DatasetsetUsagesExport datasetsetUsagesExport = new DatasetsetUsagesExport(properties);
    final String[] dois = {"10.15468/dl.7i17ji","10.15468/riwjrl"};
    for(String doi : dois) {
      System.out.println(datasetsetUsagesExport.getCitations(doi).size());
    }

  }

}
