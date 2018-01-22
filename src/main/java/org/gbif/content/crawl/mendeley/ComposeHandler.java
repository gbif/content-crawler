package org.gbif.content.crawl.mendeley;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.List;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compose handler, used to  encapsulates all response handlers.
 */
public class ComposeHandler  implements ResponseHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ComposeHandler.class);

  private final List<ResponseHandler> handlers = Lists.newArrayList();

  public ComposeHandler(ContentCrawlConfiguration config) {
    handlers.add(new ResponseToFileHandler(config.mendeley.targetDir));
    if (config.elasticSearch != null) {
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
