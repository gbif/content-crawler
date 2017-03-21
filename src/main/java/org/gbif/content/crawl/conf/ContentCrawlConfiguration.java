package org.gbif.content.crawl.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * Command configuration class, contains configuration element to connect to Mendeley, ElastisSearch and Contenful.
 */
public class ContentCrawlConfiguration {

  @NotNull
  public Mendeley mendeley;

  // Deliberately Nullable
  public ElasticSearch elasticSearch;

  public Contentful contentful;


  /**
   * Configuration specific to interfacing to Mendeley.
   */
  public static class Mendeley {

    @Parameter(
      names = "-targetDir", converter = FileConverter.class,
      description = "The target directory to store the crawl responses (JSON docs)")
    @NotNull
    public File targetDir;

    @Parameter(
      names = "-tokenUrl",
      description = "The Mendeley url for the authentication, defaulting to https://api.mendeley.com/oauth/token")
    public String tokenUrl = "https://api.mendeley.com/oauth/token";

    @Parameter(
      names = "-trustedClientId",
      description = "The client ID registered in http://dev.mendeley.com/")
    @NotNull
    public String trustedClientId;

    @Parameter(
      names = "-trustedClientSecret",
      description = "The client secret registered in http://dev.mendeley.com/")
    @NotNull
    public String trustedClientSecret;

    @Parameter(
      names = "-groupId",
      description = "The Mendeley public group id to crawl, defaulting to the GBIF Public Group")
    public String groupId = "dcb8ff61-dbc0-3519-af76-2072f22bc22f"; // immutable

    @Parameter(
      names = "-targetUrl",
      description = "The templated target URL to crawl, defaulting to https://api.mendeley.com/documents?group_id=%s&limit=500")
    public String crawlURL = "https://api.mendeley.com/documents?group_id=%s&limit=500&view=all";

    @Parameter(
      names = "-timeout",
      description = "Timeout for the HTTP calls in seconds, defaulting to 10 secs")
    public int httpTimeoutInSecs;

    public IndexBuild indexBuild;
  }

  /**
   * Configuration specific to interfacing with elastic search.
   */
  public static class ElasticSearch {
    @Parameter(
      names = "-host",
      description = "The ES node host to connect to, defaulting to localhost")
    public String host = "localhost";

    @Parameter(
      names = "-port",
      description = "The ES node port to connect to, defaulting to 9300")
    public int port = 9300;

    @Parameter(
      names = "-cluster",
      description = "The ES cluster name, defaulting to elasticsearch if none given")
    public String cluster = "elasticsearch";

  }


  /**
   * Configuration specific to interfacing with elastic search.
   */
  public static class Contentful {
    @Parameter(
      names = "-cdaToken",
      description = "Contentful authorization token")
    public String cdaToken;

    @Parameter(
      names = "-spaceId",
      description = "Contentful space Id")
    public String spaceId;

    @Parameter(
      names = "-contentTypes",
      description = "Contentful content types to be crawled")
    public List<String> contentTypes = new ArrayList<>();

    public IndexBuild indexBuild;
  }


  public static class IndexBuild {
    @Parameter(
      names = "-deleteIndex",
      description = "Delete ElasticSearch index(es) before running crawlers")
    public Boolean deleteIndex = false;

    @Parameter(
      names = "-esIndexType",
      description = "ElasticSearch index type")
    public String esIndexType = "content";


    @Parameter(
      names = "-esIndexName",
      description = "ElasticSearch index name")
    public String esIndexName;
  }

  /**
   * Converts from String to File.
   */
  private static class FileConverter implements IStringConverter<File> {
    @Override
    public File convert(String value) {
      return new File(value);
    }
  }
}
