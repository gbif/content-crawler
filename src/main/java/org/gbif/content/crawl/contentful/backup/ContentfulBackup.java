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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAArray;
import com.contentful.java.cma.model.CMAAsset;
import com.contentful.java.cma.model.CMAAssetFile;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAEntry;
import com.contentful.java.cma.model.CMASpace;
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
   * Contentful Back-up configuration is required to create an instance of this class.
   */
  public ContentfulBackup(ContentCrawlConfiguration configuration) throws IOException {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.getContentfulBackup(), "Contentful Backup configuration can't be null");
    this.configuration = configuration;
    cmaClient = buildCmaClient();
    startTime =  new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
  }

  /**
   * Executes the backup sequentially by space.
   */
  public void run() {
    try {
      CMAArray<CMASpace> result = cmaClient.spaces().fetchAll();
      result.getItems().forEach(space -> {
        LOG.info("Backing up space name[{}] id[{}]", space.getName(), space.getSpaceId());
        cmaClient.environments().fetchAll(space.getSpaceId())
          .getItems()
          .forEach(cmaEnvironment -> {
            Path spaceEnvDir = configuration.getContentfulBackup().getTargetDir().resolve(space.getSpaceId()).resolve(cmaEnvironment.getName().toLowerCase());
            backupContentTypes(space, cmaEnvironment.getEnvironmentId(), spaceEnvDir);
            backupEntries(space, cmaEnvironment.getEnvironmentId(), spaceEnvDir);
            backupAssets(space, cmaEnvironment.getEnvironmentId(), spaceEnvDir);
          });

      });
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  private void backupContentTypes(CMASpace space, String environmentId, Path spaceDir) {
    ContentfulManagementPager<CMAContentType>
      contentTypePager = ContentfulManagementPager.newContentTypePager(cmaClient, PAGE_SIZE, space.getSpaceId(), environmentId);
    Observable.fromIterable(contentTypePager)
              .doOnComplete(() -> LOG.info("Finished backing up content types"))
              .doOnError(Throwables::propagate)
              .subscribe(results ->
                results.getItems().forEach(contentType -> {

                  LOG.info("Content type id[{}]", contentType.getId());
                  // Save as e.g. ./spaceId/<timestamp>/ContentTypes/contentId.json
                  Path contentDir = spaceDir.resolve(Paths.get(startTime, "ContentType"));
                  try {
                    Files.createDirectories(contentDir);
                    Files.write(contentDir.resolve(contentType.getId() + ".json"), GSON.toJson(contentType).getBytes(StandardCharsets.UTF_8));
                  } catch (IOException e) {
                    Throwables.propagate(e);
                  }
                })
              );
  }

  private void backupEntries(CMASpace space, String environmentId, Path spaceDir) {
    ContentfulManagementPager<CMAEntry>
      entryPager = ContentfulManagementPager.newEntryPager(cmaClient, PAGE_SIZE, space.getSpaceId(), environmentId);
    Observable.fromIterable(entryPager)
              .doOnComplete(() -> LOG.info("Finished backing up entries"))
              .doOnError(Throwables::propagate)
              .subscribe(results -> {
                results.getItems().forEach(entry -> {

                  LOG.info("id[{}] of type[{}]", entry.getId(), extractContentTypeId(entry));
                  String contentTypeId = extractContentTypeId(entry);
                  String contentId = entry.getId();

                  // Save as e.g. ./spaceId/<timestamp>/contentTypeId/contentId.json
                  Path contentDir = spaceDir.resolve(Paths.get(startTime, contentTypeId));
                  try {
                    Files.createDirectories(contentDir);
                    Files.write(contentDir.resolve(contentId + ".json"), GSON.toJson(entry).getBytes(StandardCharsets.UTF_8));
                  } catch (IOException e) {
                    Throwables.propagate(e);
                  }
                });
              });
  }

  private void backupAssets(CMASpace space, String environmentId, Path spaceDir) {
    OkHttpClient client = new OkHttpClient.Builder()
      .connectTimeout(HTTP_TIMEOUT_SECS, TimeUnit.SECONDS)
      .readTimeout(HTTP_TIMEOUT_SECS, TimeUnit.SECONDS)
      .build();

    Path assetsDir = spaceDir.resolve("Asset"); // for the actual files

    ContentfulManagementPager<CMAAsset>
      assetsPager = ContentfulManagementPager.newAssetsPager(cmaClient, PAGE_SIZE, space.getSpaceId(), environmentId);
    Observable.fromIterable(assetsPager)
              .doOnComplete(() -> LOG.info("Finished backing up assets"))
              .doOnError(Throwables::propagate)
              .subscribe(results -> {
                results.getItems().forEach(asset -> {
                  LOG.info("Asset: {}", GSON.toJson(asset.getId()));

                  // Save the actual assets (files, PDFs etc) only if thet are not already saved, and always save the
                  // metadata about the asset
                  try {
                    CMAAssetFile file = asset.getFields().getFile("en-GB");
                    if (file != null) {
                      // skip any asset that has no URL as it is meaningless and indicates bad content
                      if (file.getUrl() != null) {
                        String assetUrl = file.getUrl();
                        LOG.info("File[{}] has URL[{}]", file.getFileName(), assetUrl);

                        // we map the local path to the URL path (wu1jj10r9bwp is the spaceID)
                        // //assets.contentful.com/wu1jj10r9bwp/59Vy2G95H2EIE8yOIcgSSu/ae59118ae9989c26119972f1662aff3f/test.pdf
                        //String path = assetUrl.substring(assetUrl.indexOf(spaceId) + spaceId.length() + 1);
                        Path targetFile = assetsDir.resolve(extractAssetPath(assetUrl));

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


                    // Save as e.g. ./spaceId/<timestamp>/Asset/contentId.json
                    Path contentDir = spaceDir.resolve(Paths.get(startTime, "Asset"));
                    Files.createDirectories(contentDir);
                    Files.write(contentDir.resolve(asset.getId() + ".json"), GSON.toJson(asset).getBytes(StandardCharsets.UTF_8));

                  } catch (Exception e) {
                    Throwables.propagate(e);
                  }
                });
              });
  }

  /**
   * Extracts the relative path to the file after the asset directory from a contentful asset URL.
   * e.g. given "//images.contentful.com/wu1jj10r9bwp/5JLBumd6qkmGEM4eOG0mOa/f9be6347556d16b3e22d2f42e13f6ef4/event-image-115-180.jpeg"
   * returns "/5JLBumd6qkmGEM4eOG0mOa/f9be6347556d16b3e22d2f42e13f6ef4/event-image-115-180.jpeg"
   */
  private static String extractAssetPath(String url) {
    if (url != null) {
      String cleaned = url.replaceAll("http://|https://|//", ""); // defensive coding
      String[] atoms = cleaned.split("/");
      Preconditions.checkArgument(atoms.length == 5, "Asset URL not in expected format");
      return cleaned.substring(cleaned.indexOf(atoms[2]));

    }
    return null;
  }

  // Extracts the id from e.g.
  //   sys: { contentType: {sys={type=Link, linkType=ContentType, id=Event}}}
  private static String extractContentTypeId(CMAEntry entry) {
    return entry.getSystem().getContentType().getId();
  }


  /**
   * Creates a new instance of a Contentful CMAClient.
   */
  private CMAClient buildCmaClient() {
    return new CMAClient.Builder()
      .setAccessToken(configuration.getContentfulBackup().getCmaToken())
      .build();
  }
}
