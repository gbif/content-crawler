package org.gbif.content.crawl.contentful;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAArray;
import com.contentful.java.cma.model.CMAAsset;
import com.contentful.java.cma.model.CMAEntry;
import com.contentful.java.cma.model.CMAResource;
import com.contentful.java.cma.model.CMASpace;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.reactivex.Observable;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okio.BufferedSink;
import okio.Okio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.es.ElasticSearchUtils.createIndex;

/**
 * Crawls all content from Contentful through the Management API and stores it for backup and disaster recovery
 * purposes.
 */
public class ContentfulBackup {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulBackup.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create(); // GSON since it's used internally by Contentful API
  private static final int PAGE_SIZE = 100; // note: limited to 100 by Contentful
  private static final int HTTP_TIMEOUT_SECS = 120; // note: suggest we be very lenient

  private final ContentCrawlConfiguration configuration;
  private final CMAClient cmaClient;
  private final String startTime;

  /**
   * ElasticSearch and Contentful configuration are required to create an instance of this class.
   */
  public ContentfulBackup(ContentCrawlConfiguration configuration) throws IOException {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.contentfulBackup, "Contentful Backup configuration can't be null");
    this.configuration = configuration;
    cmaClient = buildCmaClient();
    startTime =  new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) ;
  }

  /**
   * Executes the backup sequentially by space.
   */
  public void run() {
    try {
      CMAArray<CMASpace> result = cmaClient.spaces().fetchAll();
      for (CMASpace space : result.getItems()) {
        LOG.info("Backing up space name[{}] id[{}]", space.getName(), extractSysId(space));

        Path spaceDir = configuration.contentfulBackup.targetDir.resolve(extractSysId(space));

        //backupContentTypes(space, spaceDir);
        backupEntries(space, spaceDir);
        backupAssets(space, spaceDir);
      }

    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  private void backupEntries(CMASpace space, Path spaceDir) {
    ContentfulManagementPager<CMAEntry>
      entryPager = ContentfulManagementPager.newEntryPager(cmaClient, PAGE_SIZE, extractSysId(space));
    Observable.fromIterable(entryPager)
              .doOnComplete(() -> LOG.info("Finished backing up entries"))
              .doOnError(error -> Throwables.propagate(error))
              .subscribe(results -> {
                results.getItems().forEach(entry -> {

                  LOG.info("id[{}] of type[{}]", entry.getResourceId(), extractContentTypeId(entry));
                  String contentTypeId = extractContentTypeId(entry);
                  String contentId = entry.getResourceId();

                  // Save as e.g. ./spaceId/<timestamp>/contentTypeId/contentId.json
                  Path contentDir = spaceDir.resolve(Paths.get(startTime, contentTypeId));
                  try {
                    Files.createDirectories(contentDir);
                    Files.write(contentDir.resolve(contentId + ".json"), GSON.toJson(entry).getBytes("UTF8"));
                  } catch (IOException e) {
                    Throwables.propagate(e);
                  }
                });
              });
  }

  private void backupAssets(CMASpace space, Path spaceDir) {
    String spaceId = extractSysId(space);
    OkHttpClient client = new OkHttpClient.Builder()
      .connectTimeout(HTTP_TIMEOUT_SECS, TimeUnit.SECONDS)
      .readTimeout(HTTP_TIMEOUT_SECS, TimeUnit.SECONDS)
      .build();

    Path assetsDir = spaceDir.resolve("Assets"); // for the actual files

    ContentfulManagementPager<CMAAsset>
      assetsPager = ContentfulManagementPager.newAssetsPager(cmaClient, PAGE_SIZE, extractSysId(space));
    Observable.fromIterable(assetsPager)
              .doOnComplete(() -> LOG.info("Finished backing up assets"))
              .doOnError(error -> Throwables.propagate(error))
              .subscribe(results -> {
                results.getItems().forEach(asset -> {
                  LOG.info("Asset: {}", GSON.toJson(extractSysId(asset)));

                  // Save the actual assets (files, PDFs etc) only if thet are not already saved, and always save the
                  // metadata about the asset
                  try {
                    Map<String, Map> languages = (Map) asset.getFields().get("file");
                    for (Map.Entry<String, Map> e : languages.entrySet()) {

                      // skip any asset that has no URL as it is meaningless and indicates bad content
                      if (e.getValue().get("url") != null) {
                        String assetUrl = String.valueOf(e.getValue().get("url"));
                        LOG.info("Language[{}] has URL[{}]", e.getKey(), assetUrl);

                        // we map the local path to the URL path (wu1jj10r9bwp is the spaceID)
                        // //assets.contentful.com/wu1jj10r9bwp/59Vy2G95H2EIE8yOIcgSSu/ae59118ae9989c26119972f1662aff3f/test.pdf
                        String path = assetUrl.substring(assetUrl.indexOf(spaceId) + spaceId.length() + 1);
                        Path targetFile = assetsDir.resolve(path);

                        if (Files.exists(targetFile)) {
                          LOG.info("Skipping asset file which already exists: {}", targetFile.toAbsolutePath());
                        } else {
                          Files.createDirectories(targetFile.getParent()); // defensive coding
                          Files.createFile(targetFile);

                          try (BufferedSink sink = Okio.buffer(Okio.sink(targetFile))) {
                            Request request = new Request.Builder()
                              .url("http://" + assetUrl)
                              .build();

                            Response response = client.newCall(request).execute();
                            sink.writeAll(response.body().source());
                          }
                        }
                      }
                    }

                    // Save as e.g. ./spaceId/<timestamp>/Assets/contentId.json
                    Path contentDir = spaceDir.resolve(Paths.get(startTime, "Asset"));
                    Files.createDirectories(contentDir);
                    Files.write(contentDir.resolve(asset.getResourceId() + ".json"), GSON.toJson(asset).getBytes("UTF8"));

                  } catch (Exception e) {
                    Throwables.propagate(e);
                  }
                });
              });
  }

  /**
   *
   * @See https://github.com/contentful/contentful-management.java/issues/71
   * @throws NullPointerException if you provide anything other than an object with a valid sys entry
   */
  private String extractSysId(CMAResource resource) {
    return String.valueOf(resource.getSys().get("id"));
  }

  // Extracts the id from e.g.
  //   sys: { contentType: {sys={type=Link, linkType=ContentType, id=Event}}}
  private String extractContentTypeId(CMAEntry entry) {
    return (String)
      ((Map)(
          (Map)(entry.getSys().get("contentType"))
        ).get("sys"))
        .get("id");
  }


  /**
   * Creates a new instance of a Contentful CMAClient.
   */
  private CMAClient buildCmaClient() {
    return new CMAClient.Builder()
      .setAccessToken(configuration.contentfulBackup.cmaToken).build();
  }
}
