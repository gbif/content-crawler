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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Compose handler, used to  encapsulates all response handlers.
 */
public class ComposeHandler  implements ResponseHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ComposeHandler.class);

  private final List<ResponseHandler> handlers = Lists.newArrayList();

  public ComposeHandler(ContentCrawlConfiguration config) {
    handlers.add(new ResponseToFileHandler(config.getMendeley().getTargetDir()));
    if (config.getElasticSearch() != null) {
      handlers.add(new ElasticSearchIndexHandler(config));
    }
  }

  @Override
  public void handleResponse(String responseAsJson) throws Exception {
    handlers.parallelStream().forEach(handler -> {
      try {
        handler.handleResponse(responseAsJson);
      } catch (Exception e) {
        LOG.error("Unable to process response", e);
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void finish() throws Exception {
    handlers.parallelStream().forEach(handler -> {
      try {
        handler.finish();
      } catch (Exception e) {
        LOG.error("Error finishing handler", e);
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void rollback() throws Exception {
    handlers.parallelStream().forEach(handler -> {
      try {
        handler.rollback();
      } catch (Exception e) {
        LOG.error("Error rolling back handler", e);
        throw new RuntimeException(e);
      }
    });
  }
}
