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
      if (response == null || !response.isEndOfRecords()) {
        response = dataSupplier.apply(pagingRequest);
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
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
