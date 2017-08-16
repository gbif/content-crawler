package org.gbif.content.crawl.mendeley;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
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
  private static final int CRAWL_BUFFER = 10;

  private static final Logger LOG = LoggerFactory.getLogger(MendeleyDocumentCrawler.class);
  private final RequestConfig requestConfig;

  private final ContentCrawlConfiguration config;
  private final List<ResponseHandler> handlers = Lists.newArrayList();


  public MendeleyDocumentCrawler(ContentCrawlConfiguration config) {
    this.config = config;
    int timeOut = config.mendeley.httpTimeout;
    requestConfig = RequestConfig.custom().setSocketTimeout(timeOut).setConnectTimeout(timeOut)
                      .setConnectionRequestTimeout(timeOut).build();
    handlers.add(new ResponseToFileHandler(config.mendeley.targetDir));
    if (config.elasticSearch != null) {
      handlers.add(new ElasticSearchIndexHandler(config));
    }
  }

  public void run() throws IOException {
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
        .doOnTerminate(() -> handlers.forEach(handler -> {
          try {
            handler.finish();
          } catch (Exception ex) {
            LOG.error("Error finishing handlers", ex);
          }
        }))
        .subscribe(
          responses ->
            responses.parallelStream().forEach(response->
              handlers.parallelStream().forEach(handler -> {
                try {
                  handler.handleResponse(response);
                } catch (Exception e) {
                  LOG.error("Unable to process response", e);
                }
              })
        ));
    } catch (OAuthSystemException | OAuthProblemException e) {
      LOG.error("Unable ot authenticate with Mendeley", e);
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
