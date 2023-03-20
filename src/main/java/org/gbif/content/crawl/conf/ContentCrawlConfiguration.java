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
package org.gbif.content.crawl.conf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Command configuration class, contains configuration element to connect to Mendeley, ElasticSearch and Contentful.
 */
@Data
@NoArgsConstructor
public class ContentCrawlConfiguration {

  @Nullable
  private Mendeley mendeley;

  @Nullable
  private ElasticSearch elasticSearch;

  @Nullable
  private GbifApi gbifApi;

  @Nullable
  private Contentful contentful;

  @Nullable
  private ContentfulBackup contentfulBackup;

  @Nullable
  private ContentfulRestore contentfulRestore;

  /**
   * Configuration specific to interfacing to Mendeley.
   */
  @Data
  @NoArgsConstructor
  public static class Mendeley {

    @Parameter(
      names = "-targetDir", converter = FileConverter.class,
      description = "The target directory to store the crawl responses (JSON docs)")
    @NotNull
    private File targetDir;

    @Parameter(
      names = "-tokenUrl",
      description = "The Mendeley url for the authentication, defaulting to https://api.mendeley.com/oauth/token")
    private String tokenUrl = "https://api.mendeley.com/oauth/token";

    @Parameter(
      names = "-authToken",
      description = "The Mendeley Auth Token  generated from http://dev.mendeley.com/")
    @NotNull
    private String authToken;

    @Parameter(
      names = "-redirectURI",
      description = "The redirect URL registered for trusted applicaiton http://dev.mendeley.com/")
    @NotNull
    private String redirecURI;

    @Parameter(
      names = "-targetUrl",
      description = "The templated target URL to crawl, defaulting to https://api.mendeley.com/documents?&limit=500")
    private String crawlURL = "https://api.mendeley.com/documents?limit=500&view=all";

    @Parameter(
      names = "-timeout",
      description = "Timeout for the HTTP calls in seconds, defaulting to 10 secs")
    private int httpTimeout = 10000;

    @Parameter(
      names = "-controlledTags",
      description = "Tags values that must be handled as separate fields in the resulting index")
    private Map<String,List<String>> controlledTags = new HashMap<>();

    @Parameter(
      names = "-dbConfig",
      description = "Configuration to establish a connection to the Registry DB to get data of downloads and datasets")
    private Map<String,String> dbConfig = new HashMap<>();

    private IndexBuild indexBuild;

    private ElasticSearch datasetElasticSearch;

    private String datasetIndex = "dataset";
  }

  /**
   * Configuration specific to interfacing with elastic search.
   */
  @Data
  @NoArgsConstructor
  public static class ElasticSearch {
    @Parameter(
      names = "-host",
      description = "The ES node host to connect to, defaulting to localhost")
    private String host = "localhost";

    @Parameter(
      names = "-connectionTimeOut",
      description = "Connection time out")
    private int connectionTimeOut = 60000;

    @Parameter(
      names = "-socketTimeOut",
      description = "SocketTimeOut")
    private int socketTimeOut = 60000;

    @Parameter(
      names = "-connectionRequestTimeOut",
      description = "ConnectionRequestTimeOut time out")
    private int connectionRequestTimeOut = 120000;

    @Parameter(
      names = "-maxResultWindow",
      description = "MaxResultWindow ES")
    private int maxResultWindow = 35000;
  }


  /**
   * Configuration specific to interfacing with Contentful content API.
   */
  @Data
  @NoArgsConstructor
  public static class Contentful {
    @Parameter(
      names = "-cdaToken",
      description = "Contentful authorization token")
    private String cdaToken;

    @Parameter(
      names = "-cmaToken",
      description = "Contentful management access token")
    @NotNull
    private String cmaToken;

    @Parameter(
      names = "-spaceId",
      description = "Contentful space Id")
    private String spaceId;

    @Parameter(
      names = "-environmentId",
      description = "Contentful space environment")
    private String environmentId = "master";

    @Parameter(
      names = "-contentTypes",
      description = "Contentful content types to be crawled")
    private List<String> contentTypes = new ArrayList<>();

    @Parameter(
      names = "-vocabularies",
      description = "Contentful vocabularies to be crawled")
    private Set<String> vocabularies = new HashSet<>();

    @Parameter(
      names = "-countryVocabulary",
      description = "Named of the country vocabulary content, it's handled specially during indexing")
    private String countryVocabulary;

    @Parameter(
            names = "-newsContentType",
            description = "Name of the news content type, it's handled specially during indexing to tag related entities")
    private String newsContentType;

    @Parameter(
            names = "-articleContentType",
            description = "Name of the article content type, it's handled specially during indexing to tag related entities")
    private String articleContentType = "Article";

    private IndexBuild indexBuild;
  }

  @Data
  @NoArgsConstructor
  public static class IndexBuild {

    @Parameter(
      names = "-esIndexAlias",
      description = "ElasticSearch index alias")
    private String esIndexAlias = "content";

    @Parameter(
      names = "-esIndexName",
      description = "ElasticSearch index name")
    private String esIndexName;

    @Parameter(
      names = "-batchSize",
      description = "Batch size for bulk indexing")
    private int batchSize = 50;
  }

  @Data
  @NoArgsConstructor
  public static class GbifApi {
    @Parameter(
      names = "-gbifApiUrl",
      description = "URL to GBIF API")
    private String url;

    @Parameter(
      names = "-gbifApiUsername",
      description = "Username to GBIF API")
    private String username;

    @Parameter(
      names = "-gbifApiPassword",
      description = "Password for GBIF API")
    private String password;
  }

  /**
   * Configuration specific to backing up Contentful through the Contentful Management API.
   */
  @Data
  @NoArgsConstructor
  public static class ContentfulBackup {
    @Parameter(
      names = "-cmaToken",
      description = "Contentful management access token")
    @NotNull
    private String cmaToken;

    @Parameter(
      names = "-targetDir",
      description = "The target directory to save the Contentful backup")
    @NotNull
    @JsonDeserialize(using = PathDeserializer.class)
    private Path targetDir;
  }

  /**
   * Configuration specific to backing up Contentful through the Contentful Management API.
   */
  @Data
  @NoArgsConstructor
  public static class ContentfulRestore {
    @Parameter(
      names = "-cmaToken",
      description = "Contentful management access token")
    @NotNull
    private String cmaToken;

    @Parameter(
      names = "-spaceId",
      description = "Contentful spaceId to populate")
    @NotNull
    private String spaceId;

    @Parameter(
      names = "-environmentId",
      description = "Contentful space environment")
    private String environmentId = "master";

    @Parameter(
      names = "-sourceDir",
      description = "The directory containing the backup")
    @NotNull
    @JsonDeserialize(using = PathDeserializer.class)
    private Path sourceDir;
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
