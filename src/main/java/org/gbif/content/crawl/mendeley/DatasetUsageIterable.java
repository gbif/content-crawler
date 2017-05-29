package org.gbif.content.crawl.mendeley;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.OccurrenceDownloadService;

import java.util.Iterator;

/**
 * Utility class to iterate through all dataset usages of a GBIF download.
 */
public class DatasetUsageIterable implements Iterable<PagingResponse<DatasetOccurrenceDownloadUsage>> {

  private static final int PAGE_SIZE = 50;
  private final String downloadKey;
  private final OccurrenceDownloadService downloadService;

  public DatasetUsageIterable(OccurrenceDownloadService downloadService, String downloadKey) {
    this.downloadKey = downloadKey;
    this.downloadService = downloadService;
  }

  private class DataUsageIterator implements Iterator<PagingResponse<DatasetOccurrenceDownloadUsage>> {

    private PagingResponse<DatasetOccurrenceDownloadUsage> response;
    private PagingRequest pagingRequest = new PagingRequest(0, PAGE_SIZE);

    @Override
    public boolean hasNext() {
      response = downloadService.listDatasetUsages(downloadKey, pagingRequest);
      return response != null && !response.isEndOfRecords();
    }

    @Override
    public PagingResponse<DatasetOccurrenceDownloadUsage> next() {
      pagingRequest.setOffset(pagingRequest.getOffset() +  PAGE_SIZE);
      return response;
    }
  }

  @Override
  public Iterator<PagingResponse<DatasetOccurrenceDownloadUsage>> iterator() {
    return new DataUsageIterator();
  }

}
