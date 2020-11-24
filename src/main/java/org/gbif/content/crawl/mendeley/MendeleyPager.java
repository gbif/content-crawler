package org.gbif.content.crawl.mendeley;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterable utility to crawl Mendeley data.
 */
public class MendeleyPager implements Iterable<String> {

  private static final Logger LOG = LoggerFactory.getLogger(MendeleyPager.class);

  private static final Pattern NEXT_REPLACE = Pattern.compile("<|(>;\\s+rel\\=\"next\")");

  private static final String NEXT_PATTERN = "rel=\"next\"";

  private final String targetUrl;
  private final String token;
  private final RequestConfig requestConfig;
  private final CloseableHttpClient httpClient;

  /**
   * Fll constructor.
   * @param targetUrl Mendeley API url
   * @param token authentication token
   * @param requestConfig Http request configuration
   * @param httpClient closeable http client
   */
  public MendeleyPager(String targetUrl, String token,
                       RequestConfig requestConfig, CloseableHttpClient httpClient) {
    this.targetUrl = targetUrl;
    this.token = token;
    this.requestConfig = requestConfig;
    this.httpClient = httpClient;
  }

  /**
   * Iterates thought Mendeley responses.
   */
  private class MendeleyIterator implements Iterator<String> {

    private Optional<String> nextUrl;

    /**
     * Initializes the nextUrl using the initial target url.
     */
    MendeleyIterator() {
      nextUrl = Optional.ofNullable(targetUrl);
    }

    @Override
    public boolean hasNext() {
      return nextUrl.isPresent();
    }

    @Override
    public String next() {
      return nextUrl.map(targetUrl -> {
              HttpGet httpGet = new HttpGet(targetUrl + "&access_token=" + token);
              httpGet.setConfig(requestConfig);
              LOG.info("Requesting data from {}", httpGet.getURI());
              try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                if (HttpStatus.SC_OK != httpResponse.getStatusLine().getStatusCode()) {
                  LOG.warn("Mendeley returning HTTP[{}] with {}",
                           httpResponse.getStatusLine().getStatusCode(),
                           httpResponse.getStatusLine().getReasonPhrase());
                  throw new RuntimeException("Error communicating with Mendeley API");
                }

                // Useful logging for production operation
                Optional.ofNullable(httpResponse.getFirstHeader("Mendeley-Count"))
                  .ifPresent(totalResults -> LOG.info("Mendeley reports total results: {}", totalResults.getValue()));
                nextUrl = nextPageFromHeaders(httpResponse);
                return EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8.name());
              } catch (IOException ex) {
                LOG.error("Error contacting Mendeley endpoint", ex);
                throw new RuntimeException(ex);
              }
      }).orElseThrow(() -> new NoSuchElementException("No more elements to crawl"));
    }
  }


  @Override
  public Iterator<String> iterator() {
    return new MendeleyIterator();
  }

  /**
   * Extracts the URL for the next page of results from the Http Headers or null if none found.
   * @see <a href="http://dev.mendeley.com/reference/topics/pagination.html">Pagination in the Mendeley API</a>
   * @param response To extract from
   * @return The URL of the next page or null
   */
  private static Optional<String> nextPageFromHeaders(HttpResponse response) {
    return Arrays.stream(response.getHeaders("Link")).filter(link -> link.getValue().contains(NEXT_PATTERN))
      .findFirst().map(link -> NEXT_REPLACE.matcher(link.getValue()).replaceAll(""));
  }
}
