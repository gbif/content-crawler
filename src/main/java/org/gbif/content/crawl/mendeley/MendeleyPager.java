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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import io.reactivex.Observable;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
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

  private static String getParamValue(String name, String url) {
    try {
      URIBuilder b = new URIBuilder(url);
      return b.getQueryParams()
        .stream()
        .filter(p -> p.getName().equalsIgnoreCase(name))
        .map(NameValuePair::getValue)
        .findFirst()
        .orElse("");
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
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

      return nextUrl.map(nextTargetUrl -> {
              HttpGet httpGet = new HttpGet(nextTargetUrl.contains("access_token")? nextTargetUrl : nextTargetUrl + "&access_token=" + token);
              httpGet.setConfig(requestConfig);
              LOG.info("Requesting data from {} using paging marker {}", targetUrl, getParamValue("marker", nextTargetUrl));
              try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                if (HttpStatus.SC_GATEWAY_TIMEOUT == httpResponse.getStatusLine().getStatusCode()) {
                  throw new GatewayTimeoutException();
                }

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
