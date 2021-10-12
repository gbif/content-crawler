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
package org.gbif.content.crawl.contentful.crawl;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;

/**
 * Iterates over the resources of a Contentful content type.
 */
public class ContentfulPager implements Iterable<CDAArray> {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulPager.class);

  //how many nested elements should retrieved on each call
  private static final int LEVEL = 2;

  private static final String LOCALE_PARAM = "locale";

  private static final String ALL = "*";

  //Contentful client
  private final CDAClient cdaClient;

  //Number of results to retrieve
  private final int pageSize;

  //Content Type identifier
  private final String contentTypeId;

  /**
   * Iterator through the resources of a Content Type.
   */
  private class ContentfulIterator implements Iterator<CDAArray> {

    //Current page
    private int skip;

    //Current results
    private CDAArray current;

    @Override
    public boolean hasNext() {
      //fetch the next set of results
      current = cdaClient.fetch(CDAEntry.class).withContentType(contentTypeId).include(LEVEL).limit(pageSize)
                  .where(LOCALE_PARAM, ALL).skip(skip).all();
      skip += pageSize;
      return current.total() > 0;
    }

    @Override
    public CDAArray next() {
      if (current.total() == 0) {
        throw new NoSuchElementException("No more resources available");
      }
      LOG.info("Crawling content type [{}], from page [{}] to page [{}]", contentTypeId, skip, skip + pageSize);
      return current;
    }
  }

  /**
   * Full constructor.
   */
  public ContentfulPager(CDAClient cdaClient, int pageSize, String contentTypeId) {
    this.cdaClient = cdaClient;
    this.pageSize = pageSize;
    this.contentTypeId = contentTypeId;
  }

  @Override
  public Iterator<CDAArray> iterator() {
    return new ContentfulIterator();
  }
}
