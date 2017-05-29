package org.gbif.content.crawl.mendeley;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;

import java.util.Iterator;

/**
 * Utility class to iterate through all datasets associated to a DOI.
 */
public class DatasetsByDoiIterable implements Iterable<PagingResponse<Dataset>> {

  private static final int PAGE_SIZE = 50;
  private final String doi;
  private final DatasetService datasetService;

  public DatasetsByDoiIterable(DatasetService datasetService, String doi) {
    this.doi = doi;
    this.datasetService = datasetService;
  }

  private class DatasetsByDoisIterator implements Iterator<PagingResponse<Dataset>> {

    private PagingResponse<Dataset> response;
    private PagingRequest pagingRequest = new PagingRequest(0, PAGE_SIZE);

    @Override
    public boolean hasNext() {
      response = datasetService.listByDOI(doi, pagingRequest);
      return response != null && !response.isEndOfRecords();
    }

    @Override
    public PagingResponse<Dataset> next() {
      pagingRequest.setOffset(pagingRequest.getOffset() + PAGE_SIZE);
      return response;
    }
  }

  @Override
  public Iterator<PagingResponse<Dataset>> iterator() {
    return new DatasetsByDoisIterator();
  }

}
