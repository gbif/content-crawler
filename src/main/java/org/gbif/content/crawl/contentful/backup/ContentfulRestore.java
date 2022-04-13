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

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAAsset;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Populates an empty space in Contentful with a previously exported backup.
 */
public class ContentfulRestore {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulRestore.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create(); // GSON since it's used internally by Contentful API

  private final ContentCrawlConfiguration.ContentfulRestore configuration;
  private final CMAClient cmaClient;
  private final RateLimiter rateLimiter = RateLimiter.create(1); // ops/sec

  /**
   * Contentful Restore configuration is required to create an instance of this class.
   */
  public ContentfulRestore(ContentCrawlConfiguration configuration) {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.contentfulRestore, "Contentful Restore configuration can't be null");
    this.configuration = configuration.contentfulRestore;
    cmaClient = buildCmaClient();
  }

  /**
   * Executes the backup sequentially by space.
   */
  public void run() {
    try {
      restoreContentTypes();
      restoreAssets();

      // Restore all the entries, skipping the Asset meta folder
      Files.list(configuration.sourceDir)
           .forEach(path ->  {
                      if (path.endsWith("Asset") || path.endsWith("ContentType")) {
                        return;
                      }

                      try {
                        LOG.info("Starting {}", path);
                        restoreEntries(path);
                      } catch (IOException e) {
                        Throwables.propagate(e);
                      }
                  });

    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  private void restoreContentTypes() throws IOException {
    Files.list(configuration.sourceDir.resolve("./ContentType"))
         .forEach(path -> {
             try {
               CMAContentType type = GSON.fromJson(new String(Files.readAllBytes(path), StandardCharsets.UTF_8), CMAContentType.class);

               try {
                 CMAContentType existing = cmaClient.contentTypes().fetchOne(configuration.spaceId, configuration.environmentId,
                                                                             type.getId());
                 LOG.info("Content type exists: {}", existing.getName());
                 rateLimiter.acquire();

               } catch (RuntimeException e) {
                 LOG.info("Restoring content type: {}", type.getName());
                 CMAContentType created = cmaClient.contentTypes().create(configuration.spaceId, configuration.environmentId, type);

                 rateLimiter.acquire();
                 cmaClient.contentTypes().publish(created);
                 rateLimiter.acquire();
               }

             } catch (IOException e) {
               Throwables.propagate(e);
             }
         }
    );
  }

  private void restoreAssets() throws IOException {
    Path assetsDir = configuration.sourceDir.resolve("../Asset");
    Preconditions.checkArgument(Files.exists(assetsDir),
                                "Assets directory [%s] is missing and should be sitting beside the sourceDir",
                                assetsDir);
    Files.list(configuration.sourceDir.resolve("./Asset"))
         .forEach(path -> {
                    try {
                      String assetAsJSON = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                      // In the backup the URL will have the likes of this:
                      //   "url": "//images.contentful.com/
                      // If we are to do disaster recovery, we actually need to make the backup directory visible on
                      // the internet, and rewrite those to be something that contentful can access.  However, for
                      // just copying a space (e.g. Production -> Development) Contentful will recognise these as
                      // preprocessed, which is ideal for that purpose.  Full disaster recovery is not implemented, but
                      // this is the only know issue.
                      CMAAsset assetMeta = GSON.fromJson(assetAsJSON, CMAAsset.class);
                      try {
                        LOG.info("Asset exists: {}", assetMeta.getFields().getTitle("en-GB"));
                        rateLimiter.acquire();

                      } catch (RuntimeException e) {
                        LOG.info("Restoring asset: {}", assetMeta.getFields().getTitle("en-GB"));

                        CMAAsset created = cmaClient.assets().create(configuration.spaceId, configuration.environmentId, assetMeta);
                        // If you are developing a disaster recovery, here you would need to do something like this:
                        // cmaClient.assets().process(...);

                        // Assets which are not processed cannot be published
                        if (created.getFields().getFile("en-GB") != null &&
                            created.getFields().getFile("en-GB").getUrl() != null) {
                          cmaClient.assets().publish(created);
                          rateLimiter.acquire();
                        }

                      }

                    } catch (IOException e) {
                      Throwables.propagate(e);
                    }
                  }
         );
  }

  private void restoreEntries(Path entryDirectory) throws IOException {
    Files.list(entryDirectory)
         .forEach(path -> {
                      try {

                        LOG.info("Restoring {}", path.getFileName());
                        CMAEntry entry = GSON.fromJson(new String(Files.readAllBytes(path), StandardCharsets.UTF_8), CMAEntry.class);
                        try {
                          LOG.info("Entry exists: {}", getContentTypeId(entry));
                          rateLimiter.acquire();
                        } catch (RuntimeException e) {
                          rateLimiter.acquire();
                        }

                      } catch (IOException e) {
                        Throwables.propagate(e);
                      }
                  }
         );
  }

  /**
   * @return the content type id of the entry
   */
  private static String getContentTypeId(CMAEntry entry) {
    // there is no other way to extract this
    try {
      return entry.getSystem().getContentType().getId();
    } catch (Exception e) {
      // Note: this really would be exceptional
      throw new IllegalStateException("All entries MUST have contentType system metadata - corrupt backup?");
    }

  }

  /**
   * Creates a new instance of a Contentful CMAClient.
   */
  private CMAClient buildCmaClient() {
    return new CMAClient.Builder().setAccessToken(configuration.cmaToken).build();
  }
}
