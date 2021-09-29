package org.gbif.content.crawl.mendeley;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import java.util.Collection;
import java.util.Properties;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts and caches dataset usages of a GBIF DOI.
 */
class DatasetUsagesCollector {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUsagesCollector.class);

  //jdbc fetch size
  private static final int FETCH_SIZE = 18000;

  /**
   * Utility class to store information about dataset citations of GBIF DOI.
   */
  public static class DatasetCitation implements Serializable {

    private String datasetKey;
    private String publishingOrganizationKey;
    private String downloadKey;

    public DatasetCitation(String datasetKey, String publishingOrganizationKey, String downloadKey) {
      this.datasetKey = datasetKey;
      this.publishingOrganizationKey = publishingOrganizationKey;
      this.downloadKey = downloadKey;
    }

    public String getDatasetKey() {
      return datasetKey;
    }

    public void setDatasetKey(String datasetKey) {
      this.datasetKey = datasetKey;
    }

    public String getPublishingOrganizationKey() {
      return publishingOrganizationKey;
    }

    public void setPublishingOrganizationKey(String publishingOrganizationKey) {
      this.publishingOrganizationKey = publishingOrganizationKey;
    }

    public String getDownloadKey() {
      return downloadKey;
    }

    public void setDownloadKey(String downloadKey) {
      this.downloadKey = downloadKey;
    }
  }

  //Query to extract DOI related datasets
  private static final String DATASETS_DOWNLOADS_QUERY = "SELECT od.doi, doc.dataset_key, d.publishing_organization_key, od.key AS download_key "
                                                         + "FROM occurrence_download od "
                                                         + "LEFT JOIN dataset_occurrence_download doc ON od.key = doc.download_key "
                                                         + "LEFT JOIN dataset d ON d.key = doc.dataset_key "
                                                         + "WHERE od.doi = ? "
                                                         + "UNION "
                                                         + "SELECT d.doi, d.key AS dataset_key, d.publishing_organization_key, NULL as download_key "
                                                         + "FROM dataset d "
                                                         + "WHERE d.doi = ? "
                                                         + "UNION "
                                                         + "SELECT d.doi, d.key AS dataset_key, d.publishing_organization_key, NULL as download_key "
                                                         + "FROM dataset d "
                                                         + "LEFT JOIN dataset_identifier di ON di.dataset_key = d.key "
                                                         + "LEFT JOIN identifier i ON di.identifier_key = i.key AND i.type = 'DOI' "
                                                         + "WHERE i.identifier = ? "
                                                         + "UNION "
                                                         + "SELECT d.doi, dataset_key, d.publishing_organization_key, NULL AS download_key "
                                                         + "FROM dataset_derived_dataset ddd "
                                                         + "LEFT JOIN dataset d ON d.key = ddd.dataset_key "
                                                         + "WHERE ddd.derived_dataset_doi = ?";

  private static final String IS_DERIVED_DATASET = "SELECT dd.doi FROM derived_dataset dd WHERE doi = ?";

  //Caches information by DOI
  private final Cache<String, Collection<DatasetCitation>> cache;

  //Hikari datasource
  private final DataSource dataSource;

  /**
   * Creates an instance using the required Hikari Database information.
   * @param configuration datasource configuration
   */
  public DatasetUsagesCollector(Properties configuration) {
    dataSource = initDataSource(configuration);
    cache = new Cache2kBuilder<String,Collection<DatasetCitation>>(){}
              .loader(new CacheLoader<String, Collection<DatasetCitation>>() {
                @Override
                public Collection<DatasetCitation> load(final String key) throws Exception {
                  return loadCitations(key);
                }
              }).build();
  }

  /**
   * Initialize the Hikari datasource.
   * @param properties Hikari config
   */
  private DataSource initDataSource(Properties properties) {
    HikariConfig config = new HikariConfig(properties);
    return new HikariDataSource(config);
  }

  /**
   * Cache loader function.
   */
  private Collection<DatasetCitation> loadCitations(String doi) {
    Collection<DatasetCitation> citations = new ArrayList<>();
    int resultCount = 0;
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(DATASETS_DOWNLOADS_QUERY)) {
      preparedStatement.setFetchSize(FETCH_SIZE);
      preparedStatement.setString(1, doi);
      preparedStatement.setString(2, doi);
      preparedStatement.setString(3, doi);
      preparedStatement.setString(4, doi);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          citations.add(new DatasetCitation(resultSet.getString("dataset_key"),
                                            resultSet.getString("publishing_organization_key"),
                                            resultSet.getString("download_key")));
          resultCount += 1;
        }
        LOG.info("DOI {} has {} datasets/downloads", doi, resultCount);
      }
      return citations;
    } catch (SQLException ex) {
      LOG.error("Error querying database", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Gets the dataset information of GBIF registered DOI.
   * @param doi to lookup
   * @return dataset citations associated to a DOI
   */
  public Collection<DatasetCitation> getCitations(String doi) {
    return cache.get(doi);
  }

  public boolean isDerivedDataset(String doi) {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(IS_DERIVED_DATASET)) {
      preparedStatement.setString(1, doi);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
         return resultSet.next();
      }
    } catch (SQLException ex) {
      LOG.error("Error querying database", ex);
      throw new RuntimeException(ex);
    }
  }

}
