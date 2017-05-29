package org.gbif.content.crawl.mendeley;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Utility class to iterate through a generic pageable service.
 */
public class PageableIterable<T> implements Iterable<PagingResponse<T>> {

  private static final int PAGE_SIZE = 50;
  private final Function<PagingRequest,PagingResponse<T>> dataSupplier;

  private PageableIterable(Function<PagingRequest,PagingResponse<T>> dataSupplier) {
    this.dataSupplier = dataSupplier;
  }

  private class PageableIterator implements Iterator<PagingResponse<T>> {

    private PagingResponse<T> response;
    private PagingRequest pagingRequest = new PagingRequest(0, PAGE_SIZE);

    @Override
    public boolean hasNext() {
      response = dataSupplier.apply(pagingRequest);
      return response != null && !response.isEndOfRecords();
    }

    @Override
    public PagingResponse<T> next() {
      pagingRequest.setOffset(pagingRequest.getOffset() + PAGE_SIZE);
      return response;
    }
  }

  @Override
  public Iterator<PagingResponse<T>> iterator() {
    return new PageableIterator();
  }

  public static <P> PageableIterable<P>of(Function<PagingRequest,PagingResponse<P>> supplier) {
    return new PageableIterable<>(supplier);
  }

}
