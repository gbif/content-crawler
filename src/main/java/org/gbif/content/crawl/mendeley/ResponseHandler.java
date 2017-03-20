package org.gbif.content.crawl.mendeley;

/**
 * A simple definition for handling of responses to allow them to be chained.
 */
interface ResponseHandler {
  void handleResponse(String responseAsJson) throws Exception;
  void finish() throws Exception;
}
