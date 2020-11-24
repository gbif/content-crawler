package org.gbif.content.crawl.conf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;

/**
 * Command configuration class, contains configuration element to connect to Mendeley, ElastisSearch and Contentful.
 */
public class ContentCrawlConfiguration {

  @Nullable
  public Mendeley mendeley;

  @Nullable
  public ElasticSearch elasticSearch;

  @Nullable
  public Contentful contentful;

  @Nullable
  public ContentfulBackup contentfulBackup;

  @Nullable
  public ContentfulRestore contentfulRestore;

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
      names = "-authToken",
      description = "The Mendeley Auth Token  generated from http://dev.mendeley.com/")
    @NotNull
    public String authToken;

    @Parameter(
      names = "-redirectURI",
      description = "The redirect URL registered for trusted applicaiton http://dev.mendeley.com/")
    @NotNull
    public String redirecURI;

    @Parameter(
      names = "-targetUrl",
      description = "The templated target URL to crawl, defaulting to https://api.mendeley.com/documents?&limit=500")
    public String crawlURL = "https://api.mendeley.com/documents?limit=500&view=all";

    @Parameter(
      names = "-timeout",
      description = "Timeout for the HTTP calls in seconds, defaulting to 10 secs")
    public int httpTimeout = 10000;

    @Parameter(
      names = "-gbifApiUrl",
      description = "URL to GBIF API")
    public String gbifApiUrl;

    @Parameter(
      names = "-controlledTags",
      description = "Tags values that must be handled as separate fields in the resulting index")
    public Map<String,List<String>> controlledTags = new HashMap<>();

    @Parameter(
      names = "-dbConfig",
      description = "Configuration to establish a connection to the Registry DB to get data of downloads and datasets")
    public Map<String,String> dbConfig = new HashMap<>();

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
      names = "-connectionTimeOut",
      description = "Connection time out")
    public int connectionTimeOut = 60000;

    @Parameter(
      names = "-socketTimeOut",
      description = "SocketTimeOut")
    public int socketTimeOut = 60000;

    @Parameter(
      names = "-connectionRequestTimeOut",
      description = "ConnectionRequestTimeOut time out")
    public int connectionRequestTimeOut = 120000;
  }


  /**
   * Configuration specific to interfacing with Contentful content API.
   */
  public static class Contentful {
    @Parameter(
      names = "-cdaToken",
      description = "Contentful authorization token")
    public String cdaToken;

    @Parameter(
      names = "-cmaToken",
      description = "Contentful management access token")
    @NotNull
    public String cmaToken;

    @Parameter(
      names = "-spaceId",
      description = "Contentful space Id")
    public String spaceId;

    @Parameter(
      names = "-contentTypes",
      description = "Contentful content types to be crawled")
    public List<String> contentTypes = new LinkedList<>();

    @Parameter(
      names = "-vocabularies",
      description = "Contentful vocabularies to be crawled")
    public Set<String> vocabularies = new HashSet<>();

    @Parameter(
      names = "-countryVocabulary",
      description = "Named of the country vocabulary content, it's handled specially during indexing")
    public String countryVocabulary;

    @Parameter(
            names = "-newsContentType",
            description = "Name of the news content type, it's handled specially during indexing to tag related entities")
    public String newsContentType;

    @Parameter(
            names = "-articleContentType",
            description = "Name of the article content type, it's handled specially during indexing to tag related entities")
    public String articleContentType = "Article";

    public IndexBuild indexBuild;
  }


  public static class IndexBuild {

    @Parameter(
      names = "-esIndexType",
      description = "ElasticSearch index type")
    public String esIndexType = "content";


    @Parameter(
      names = "-esIndexName",
      description = "ElasticSearch index name")
    public String esIndexName;

    @Parameter(
      names = "-batchSize",
      description = "Batch size for bulk indexing")
    public int batchSize = 50;
  }

  /**
   * Configuration specific to backing up Contentful through the Contentful Management API.
   */
  public static class ContentfulBackup {
    @Parameter(
      names = "-cmaToken",
      description = "Contentful management access token")
    @NotNull
    public String cmaToken;

    @Parameter(
      names = "-targetDir",
      description = "The target directory to save the Contentful backup")
    @NotNull
    @JsonDeserialize(using = PathDeserializer.class)
    public Path targetDir;
  }

  /**
   * Configuration specific to backing up Contentful through the Contentful Management API.
   */
  public static class ContentfulRestore {
    @Parameter(
      names = "-cmaToken",
      description = "Contentful management access token")
    @NotNull
    public String cmaToken;

    @Parameter(
      names = "-spaceId",
      description = "Contentful spaceId to populate")
    @NotNull
    public String spaceId;

    @Parameter(
      names = "-sourceDir",
      description = "The directory containing the backup")
    @NotNull
    @JsonDeserialize(using = PathDeserializer.class)
    public Path sourceDir;
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

  /**
   * Converts from String to Path (Note that Jackson needs this before JCommander gets control)
   */
  private static class PathDeserializer extends StdScalarDeserializer<Path> {
    public PathDeserializer() { super(Path.class); }

    @Override
    public Path deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext
    ) throws IOException, JsonProcessingException {
      return Paths.get(jsonParser.getValueAsString());
    }
  }

}
