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
package org.gbif.content.crawl.contentful.backup;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAArray;
import com.contentful.java.cma.model.CMAAsset;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAEntry;
import com.contentful.java.cma.model.CMAResource;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Iterates over the entries or assets of a Contentful space from the Management API.
 *
 * Note: The contentful class hierarchy lacks interfaces which is why this internally uses a "Mode" and hides this
 * through only allowing construction through static factories.
 */
class ContentfulManagementPager<T extends CMAResource> implements Iterable<CMAArray<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulManagementPager.class);

  private enum Mode {ASSETS, ENTRIES, CONTENT_TYPES}; // nuisance because of the Contentful API structure

  private final CMAClient cmaClient;
  private final Mode mode;
  private final int pageSize;
  private final String spaceId;

  // Hidden because of the "mode"
  private ContentfulManagementPager(CMAClient cmaClient, int pageSize, String spaceId, Mode mode) {
    this.pageSize = pageSize;
    this.spaceId = spaceId;
    this.cmaClient = cmaClient;
    this.mode = mode;
  }

  static ContentfulManagementPager<CMAEntry> newEntryPager(CMAClient client, int pageSize, String spaceId) {
    return new ContentfulManagementPager<>(client, pageSize, spaceId, Mode.ENTRIES);
  }

  static ContentfulManagementPager<CMAAsset> newAssetsPager(CMAClient client, int pageSize, String spaceId) {
    return new ContentfulManagementPager<>(client, pageSize, spaceId, Mode.ASSETS);
  }

  static ContentfulManagementPager<CMAContentType> newContentTypePager(CMAClient client, int pageSize, String spaceId) {
    return new ContentfulManagementPager<>(client, pageSize, spaceId, Mode.CONTENT_TYPES);
  }

  private class ContentfulIterator<T extends CMAResource> implements Iterator<CMAArray<T>> {
    private int skip; // current page skip
    private CMAArray<T> current;

    @Override
    public boolean hasNext() {
      Map<String, String> query = Maps.newHashMap(ImmutableMap.of("skip", String.valueOf(skip)));

      // This is a nuisance, but the Contentful API doesn't share a common interface
      if (Mode.ENTRIES == mode) {
        current = (CMAArray<T>) cmaClient.entries().fetchAll(spaceId, query);
      } else if (Mode.ASSETS == mode) {
        current = (CMAArray<T>) cmaClient.assets().fetchAll(spaceId, query);
      } else if (Mode.CONTENT_TYPES == mode) {
        current = (CMAArray<T>) cmaClient.contentTypes().fetchAll(spaceId, query);
      } else {
        throw new IllegalStateException("Unsupported mode of operation"); // should never happen
      }

      skip += pageSize;
      return current.getItems()!=null && !current.getItems().isEmpty();
    }

    @Override
    public CMAArray<T> next() {
      if (current.getTotal() == 0) {
        throw new NoSuchElementException("No more resources available");
      }
      LOG.info("Retrieving from page [{}] to page [{}]", skip, skip + pageSize);
      return current;
    }
  }

  @Override
  public Iterator<CMAArray<T>> iterator() {
    return new ContentfulIterator();
  }
}
