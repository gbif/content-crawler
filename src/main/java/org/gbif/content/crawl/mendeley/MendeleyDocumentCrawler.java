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

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import io.reactivex.Observable;

/**
 * A crawler of Mendeley documents storing the results in JSON files, and optionally in an Elastic Search index.
 */
public class MendeleyDocumentCrawler {

  //Buffer to use in Observables to accumulate results before handle them
  private static final int CRAWL_BUFFER = 2;

  private static final Logger LOG = LoggerFactory.getLogger(MendeleyDocumentCrawler.class);
  private final RequestConfig requestConfig;

  private final ContentCrawlConfiguration config;
  private final ResponseToFileHandler handler;


  public MendeleyDocumentCrawler(ContentCrawlConfiguration config) {

    this.config = config;
    int timeOut = config.getMendeley().getHttpTimeout();
    requestConfig = RequestConfig.custom().setSocketTimeout(timeOut).setConnectTimeout(timeOut)
                      .setConnectionRequestTimeout(timeOut).build();
    handler = new ResponseToFileHandler(config.getMendeley().getTargetDir());

  }

  public void run() throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    String targetUrl = config.getMendeley().getCrawlURL();
    LOG.info("Initiating paging crawl of {} to {}", targetUrl, config.getMendeley().getTargetDir());
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      //OAuthJSONAccessTokenResponse token = getToken(config.mendeley);
      Observable
        .fromIterable(new MendeleyPager(targetUrl, config.getMendeley().getAuthToken(), requestConfig, httpClient))
        .doOnError(err -> {
          LOG.error("Error crawling Mendeley", err);
          throw new RuntimeException(err);
        })
        .buffer(CRAWL_BUFFER)
        .doOnComplete(() -> {
          handler.finish();
          LOG.info("Time elapsed retrieving Mendeley {} minutes ", stopwatch.elapsed(TimeUnit.MINUTES));
          stopwatch.reset();
          stopwatch.start();
          indexFiles();
          LOG.info("Time elapsed indexing Mendeley {} minutes ", stopwatch.elapsed(TimeUnit.MINUTES));
          stopwatch.reset();
          stopwatch.start();
          registryFiles();
          LOG.info("Time elapsed updating GBIF Registry {} minutes ", stopwatch.elapsed(TimeUnit.MINUTES));
          stopwatch.stop();
        })
        .subscribe(
          responses ->
            responses.forEach(response -> {
                try {
                  handler.handleResponse(response);
                } catch (Exception e) {
                  LOG.error("Unable to process response", e);
                  silentRollback(handler);
                  throw new RuntimeException(e);
                }
              })
        );
    } catch (Exception e) {
      LOG.error("Unable to authenticate with Mendeley", e);
      throw new IOException("Unable to authenticate with Mendeley", e);
    }
  }

  private void indexFiles() throws Exception {
    ElasticSearchIndexHandler elasticSearchIndexHandler = new ElasticSearchIndexHandler(config);
    try {
      for (File file : handler.getTargetDir().listFiles()) {
        elasticSearchIndexHandler.handleResponse(new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
      }
      elasticSearchIndexHandler.finish();
    } catch (Exception ex) {
      elasticSearchIndexHandler.rollback();
    }
  }

  private void registryFiles() throws Exception {
    UpdateRegistryHandler updateRegistryHandler = new UpdateRegistryHandler(config);
    try {
      for (File file : handler.getTargetDir().listFiles()) {
        updateRegistryHandler.handleResponse(new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
      }
      updateRegistryHandler.finish();
    } catch (Exception ex) {
      updateRegistryHandler.rollback();
    }
  }

  /**
   * Calls the rollback and throw a runtime error.
   * @param responseHandler callback handler
   */
  private static void silentRollback(ResponseHandler responseHandler) {
    try {
      responseHandler.rollback();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
