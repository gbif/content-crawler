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

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parses the documents from the response and marks cited GBIF downloads for indefinite retention.
 */
public class UpdateRegistryHandler implements ResponseHandler {

  // Mendeley fields used by this handler
  private static final String ML_ID_FL = "id";
  private static final String ML_TAGS_FL = "tags";

  private static final String GBIF_DOWNLOAD_DOI_TAG = "gbifDOI:";
  private static final Pattern GBIF_DOWNLOAD_DOI_TAG_PATTERN =
    Pattern.compile(GBIF_DOWNLOAD_DOI_TAG+"10.15468/dl.", Pattern.LITERAL | Pattern.CASE_INSENSITIVE);

  private static final Logger LOG = LoggerFactory.getLogger(UpdateRegistryHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final DatasetUsagesCollector datasetUsagesCollector;

  public UpdateRegistryHandler(ContentCrawlConfiguration conf) {
    LOG.info("Connecting to GBIF API {} as {}", conf.getGbifApi().getUrl(), conf.getGbifApi().getUsername());
    ClientBuilder clientBuilder = new ClientBuilder()
                                    .withUrl(conf.getGbifApi().getUrl())
                                    .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
                                    .withCredentials(conf.getGbifApi().getUsername(), conf.getGbifApi().getPassword());
    occurrenceDownloadService = clientBuilder.build(OccurrenceDownloadClient.class);

    Properties dbConfig = new Properties();
    dbConfig.putAll(conf.getMendeley().getDbConfig());
    datasetUsagesCollector = new DatasetUsagesCollector(dbConfig);
  }

  /**
   * Removes the erase after date on all cited downloads.
   * @param responseAsJson To load.
   */
  @Override
  public void handleResponse(String responseAsJson) throws IOException {
    //process each Json node
    Iterable<JsonNode> iterable = () -> {
      try {
        return MAPPER.readTree(responseAsJson).elements();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };

    iterable.forEach(document -> {
      try {
        if (document.has(ML_TAGS_FL)) {
          document.get(ML_TAGS_FL).elements().forEachRemaining(node -> {
            String value = node.textValue();
            if (value.startsWith(GBIF_DOWNLOAD_DOI_TAG_PATTERN.pattern())) {
              String keyValue = value.replace(GBIF_DOWNLOAD_DOI_TAG, "").toLowerCase(Locale.ENGLISH);
              Collection<DatasetUsagesCollector.DownloadCitation> citations = datasetUsagesCollector.getDownloadCitations(keyValue);
              if (citations.isEmpty()) {
                LOG.warn("Document ID {} has an unknown DOI {}", document.get(ML_ID_FL), keyValue);
              } else {
                for (DatasetUsagesCollector.DownloadCitation citation : citations) {
                  if (!(citation.getEraseAfter() == null)) {
                    Download download = occurrenceDownloadService.get(citation.getDownloadKey());
                    if (download != null) {
                      LOG.info("Setting download {} ({}) to be retained due to citation by {}",
                               download.getKey(),
                               download.getDoi(),
                               document.get(ML_ID_FL));
                      download.setEraseAfter(null);
                      occurrenceDownloadService.update(download);
                      citation.setEraseAfter(null);
                    }
                  } else {
                    LOG.trace("Download {} already marked for retention", citation.getDownloadKey());
                  }
                }
              }
            }
          });
        } else {
          LOG.debug("No ML_TAGS_FL, {}", document);
        }
      } catch (Exception ex) {
        LOG.error("Error processing document [{}]", document, ex);
      }
    });
  }

  @Override
  public void rollback() throws Exception {
  }

  @Override
  public void finish() {
  }
}
