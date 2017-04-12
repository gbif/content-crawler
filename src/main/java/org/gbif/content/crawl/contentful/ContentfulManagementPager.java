package org.gbif.content.crawl.contentful;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.ModuleAssets;
import com.contentful.java.cma.ModuleEntries;
import com.contentful.java.cma.model.CMAArray;
import com.contentful.java.cma.model.CMAAsset;
import com.contentful.java.cma.model.CMAEntry;
import com.contentful.java.cma.model.CMAResource;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates over the entries or assets of a Contentful space from the Management API.
 *
 * Note: The contentful class hierarchy lacks interfaces which is why this internally uses a "Mode" and hides this
 * through only allowing construction through static factories.
 */
class ContentfulManagementPager<T extends CMAResource> implements Iterable<CMAArray<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulManagementPager.class);

  private enum Mode {ASSETS, ENTRIES}; // hack because of the Contentful API structure

  private final ModuleEntries entriesModule;
  private final ModuleAssets assetsModule;
  private final Mode mode;
  private final int pageSize;
  private final String spaceId;

  // Hidden because of the "mode"
  private ContentfulManagementPager(CMAClient cmaClient, int pageSize, String spaceId, Mode mode) {
    this.pageSize = pageSize;
    this.spaceId = spaceId;
    this.entriesModule = cmaClient.entries();
    this.assetsModule = cmaClient.assets();
    this.mode = mode;
  }

  static ContentfulManagementPager<CMAEntry> newEntryPager(CMAClient client, int pageSize, String spaceId) {
    return new ContentfulManagementPager<CMAEntry>(client, pageSize, spaceId, Mode.ENTRIES);
  }

  static ContentfulManagementPager<CMAAsset> newAssetsPager(CMAClient client, int pageSize, String spaceId) {
    return new ContentfulManagementPager<CMAAsset>(client, pageSize, spaceId, Mode.ASSETS);
  }


  private class ContentfulIterator<T extends CMAResource> implements Iterator<CMAArray<T>> {
    private int skip; // current page skip
    private CMAArray<T> current;

    @Override
    public boolean hasNext() {
      Map<String, String> query = Maps.newHashMap(ImmutableMap.of("skip", String.valueOf(skip)));

      if (Mode.ENTRIES == mode)
        current = (CMAArray<T>) entriesModule.fetchAll(spaceId, query);
      else if (Mode.ASSETS == mode)
        current = (CMAArray<T>) assetsModule.fetchAll(spaceId, query);
      else
        throw new IllegalStateException("Unsupported mode of operation"); // should never happen

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
