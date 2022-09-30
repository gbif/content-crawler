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
package org.gbif.content.crawl.mendeley;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.Builder;
import lombok.Data;

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
  @Data
  @Builder
  public static class DatasetCitation implements Serializable {

    private String datasetKey;
    private String publishingOrganizationKey;
    private String downloadKey;
    private Date eraseAfter;
    private UUID[] networkKeys;

  }

  /**
   * Utility class to store information about download citations (without datasets) of GBIF DOI.
   */
  @Data
  @Builder
  public static class DownloadCitation implements Serializable {

    private String downloadKey;
    private Date eraseAfter;

  }

  //Query to extract DOI related datasets
  private static final String DATASETS_DOWNLOADS_QUERY = ""
    + "SELECT od.doi, doc.dataset_key, d.publishing_organization_key, od.key AS download_key, od.erase_after, "
    + "ARRAY(SELECT nk.network_key FROM dataset_network AS nk JOIN network n ON n.key = nk.network_key AND n.deleted IS NULL WHERE nk.dataset_key = doc.dataset_key) AS network_keys "
    + "FROM occurrence_download od "
    + "LEFT JOIN dataset_occurrence_download doc ON od.key = doc.download_key "
    + "LEFT JOIN dataset d ON d.key = doc.dataset_key "
    + "WHERE od.doi = ? "
    + ""
    + "UNION "
    + "SELECT d.doi, d.key AS dataset_key, d.publishing_organization_key, NULL as download_key, NULL as erase_after, "
    + "ARRAY(SELECT nk.network_key FROM dataset_network AS nk JOIN network n ON n.key = nk.network_key AND n.deleted IS NULL WHERE nk.dataset_key = d.key) AS network_keys "
    + "FROM dataset d "
    + "WHERE d.doi = ? "
    + ""
    + "UNION "
    + "SELECT d.doi, d.key AS dataset_key, d.publishing_organization_key, NULL as download_key, NULL as erase_after, "
    + "ARRAY(SELECT nk.network_key FROM dataset_network AS nk JOIN network n ON n.key = nk.network_key AND n.deleted IS NULL WHERE nk.dataset_key = d.key) AS network_keys "
    + "FROM dataset d "
    + "LEFT JOIN dataset_identifier di ON di.dataset_key = d.key "
    + "LEFT JOIN identifier i ON di.identifier_key = i.key AND i.type = 'DOI' "
    + "WHERE i.identifier = ? "
    + ""
    + "UNION "
    + "SELECT d.doi, dataset_key, d.publishing_organization_key, NULL AS download_key, NULL as erase_after, "
    + "ARRAY(SELECT nk.network_key FROM dataset_network AS nk JOIN network n ON n.key = nk.network_key AND n.deleted IS NULL WHERE nk.dataset_key = d.key) AS network_keys "
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
                  .loader(this::loadCitations)
                  .build();
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
          citations.add(DatasetCitation.builder()
                          .datasetKey(resultSet.getString("dataset_key"))
                          .publishingOrganizationKey(resultSet.getString("publishing_organization_key"))
                          .downloadKey(resultSet.getString("download_key"))
                          .eraseAfter(resultSet.getDate("erase_after"))
                          .networkKeys((UUID[])resultSet.getArray("network_keys").getArray())
                          .build());
          resultCount += 1;
        }
        LOG.debug("DOI {} has {} datasets/downloads", doi, resultCount);
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

  /**
   * Gets the dataset information of GBIF registered DOI.
   * @param doi to lookup
   * @return dataset citations associated to a DOI
   */
  public Collection<DownloadCitation> getDownloadCitations(String doi) {
    return cache.get(doi).stream()
      .filter(c -> c.downloadKey != null)
      .map(c -> DownloadCitation.builder()
                  .downloadKey(c.downloadKey)
                  .eraseAfter(c.eraseAfter)
                  .build())
      .collect(Collectors.toSet());
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
