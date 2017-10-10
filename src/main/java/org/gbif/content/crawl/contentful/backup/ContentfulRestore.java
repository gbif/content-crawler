package org.gbif.content.crawl.contentful.backup;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAAsset;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
               CMAContentType type = GSON.fromJson(new String(Files.readAllBytes(path)), CMAContentType.class);
               cleanSys(type.getSys());

               try {
                 CMAContentType existing = cmaClient.contentTypes().fetchOne(configuration.spaceId, type.getResourceId());
                 LOG.info("Content type exists: {}", existing.getName());
                 rateLimiter.acquire();

               } catch (RuntimeException e) {
                 LOG.info("Restoring content type: {}", type.getName());
                 CMAContentType created = cmaClient.contentTypes().create(configuration.spaceId, type);

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
                      String assetAsJSON = new String(Files.readAllBytes(path));
                      // In the backup the URL will have the likes of this:
                      //   "url": "//images.contentful.com/
                      // If we are to do disaster recovery, we actually need to make the backup directory visible on
                      // the internet, and rewrite those to be something that contentful can access.  However, for
                      // just copying a space (e.g. Production -> Development) Contentful will recognise these as
                      // preprocessed, which is ideal for that purpose.  Full disaster recovery is not implemented, but
                      // this is the only know issue.
                      CMAAsset assetMeta = GSON.fromJson(assetAsJSON, CMAAsset.class);
                      cleanSys(assetMeta.getSys());

                      try {
                        CMAAsset existing = cmaClient.assets().fetchOne(configuration.spaceId, assetMeta.getResourceId());
                        LOG.info("Asset exists: {}", assetMeta.getFields().get("title").get("en-GB"));
                        rateLimiter.acquire();

                      } catch (RuntimeException e) {
                        LOG.info("Restoring asset: {}", assetMeta.getFields().get("title").get("en-GB"));

                        CMAAsset created = cmaClient.assets().create(configuration.spaceId, assetMeta);
                        // If you are developing a disaster recovery, here you would need to do something like this:
                        // cmaClient.assets().process(...);

                        // Assets which are not processed cannot be published
                        if (created.getFields().get("file") != null &&
                            created.getFields().get("file").get("en-GB") != null &&
                            ((Map<String, String>)created.getFields().get("file").get("en-GB")).get("url") != null) {
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
                        CMAEntry entry = GSON.fromJson(new String(Files.readAllBytes(path)), CMAEntry.class);
                        cleanSys(entry.getSys());

                        try {
                          CMAEntry existing = cmaClient.entries().fetchOne(configuration.spaceId, entry.getResourceId());
                          LOG.info("Entry exists: {}", getContentTypeId(entry));
                          rateLimiter.acquire();

                        } catch (RuntimeException e) {
                          CMAEntry created = cmaClient.entries().create(configuration.spaceId, getContentTypeId(entry), entry);
                          rateLimiter.acquire();
                          // TODO:
                          //cmaClient.entries().publish(created);
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
      return (String)((Map<String, Object>)((Map<String, Object>)entry.getSys().get("contentType"))
        .get("sys")).get("id");
    } catch (Exception e) {
      // Note: this really would be exceptional
      throw new IllegalStateException("All entries MUST have contentType system metadata - corrupt backup?");
    }

  }

  /**
   * Clean up the Contentful Sys metadata suitable for reposting into the new space.
   */
  private static void cleanSys(Map<String, Object> sys) {
    sys.remove("space"); // will change by definition of this restore function
    sys.remove("publishedBy"); // user might not exist
    sys.remove("updatedBy"); // user might not exist
    sys.remove("createdBy"); // user might not exist
  }

  /**
   * Creates a new instance of a Contentful CMAClient.
   */
  private CMAClient buildCmaClient() {
    return new CMAClient.Builder().setAccessToken(configuration.cmaToken).build();
  }
}
