package org.gbif.content.crawl.mendeley;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import io.reactivex.Observable;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    int timeOut = config.mendeley.httpTimeout;
    requestConfig = RequestConfig.custom().setSocketTimeout(timeOut).setConnectTimeout(timeOut)
                      .setConnectionRequestTimeout(timeOut).build();
    handler = new ResponseToFileHandler(config.mendeley.targetDir);

  }

  public void run() throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    String targetUrl = String.format(config.mendeley.crawlURL, config.mendeley.groupId);
    LOG.info("Initiating paging crawl of {} to {}", targetUrl, config.mendeley.targetDir);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      OAuthJSONAccessTokenResponse token = getToken(config.mendeley);
      Observable
        .fromIterable(new MendeleyPager(targetUrl, token, requestConfig, httpClient))
        .doOnError(err -> {
          LOG.error("Error crawling Mendeley", err);
          throw new RuntimeException(err); })
        .buffer(CRAWL_BUFFER)
        .doOnComplete(() -> {
          handler.finish();
          indexFiles();
          stopwatch.stop();
          LOG.info("Time elapsed indexing Mendeley {} minutes ", stopwatch.elapsed(TimeUnit.MINUTES));
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
    } catch (OAuthSystemException | OAuthProblemException e) {
      LOG.error("Unable ot authenticate with Mendeley", e);
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

  /**
   * Calls the rollback and throw a runtime error.
   * @param responseHandler
   */
  private static void silentRollback(ResponseHandler responseHandler) {
    try {
      responseHandler.rollback();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


  /**
   * Authenticates against the Mendeley and provides a time bound token with which to sign subsequent requests.
   *
   * @param conf Holding the configuration for accessing Mendeley
   * @return A token which will be limited in duration controlled by Mendeley.
   * @throws OAuthSystemException On issue negotiating with Mendeley Auth servers
   * @throws OAuthProblemException On configuration issue
   */
  private static OAuthJSONAccessTokenResponse getToken(ContentCrawlConfiguration.Mendeley conf)
    throws OAuthSystemException, OAuthProblemException {
    OAuthClientRequest request = OAuthClientRequest
      .tokenLocation(conf.tokenUrl)
      .setClientId(conf.trustedClientId)
      .setClientSecret(conf.trustedClientSecret)
      .setGrantType(GrantType.CLIENT_CREDENTIALS)
      .setScope("all")
      .buildBodyMessage();

    OAuthClient oAuthClient = new OAuthClient(new URLConnectionClient());
    OAuthJSONAccessTokenResponse tokenResponse = oAuthClient.accessToken(request, OAuthJSONAccessTokenResponse.class);
    oAuthClient.shutdown();
    return  tokenResponse;
  }
}
