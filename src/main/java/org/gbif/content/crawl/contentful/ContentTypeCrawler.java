package org.gbif.content.crawl.contentful;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAAsset;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.CDAField;
import com.contentful.java.cda.CDAType;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import io.reactivex.Observable;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.contentful.MappingGenerator.COLLAPSIBLE_TYPES;
import static org.gbif.content.crawl.es.ElasticSearchUtils.buildEsClient;
import static org.gbif.content.crawl.es.ElasticSearchUtils.createIndex;

/**
 * Pulls content from Contentful and stores it in ElasticSearch indexes.
 */
public class ContentTypeCrawler {

  private static final Logger LOG = LoggerFactory.getLogger(ContentTypeCrawler.class);

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  private static final String CONTENT_TYPE_FIELD = "contentType";
  private static final String REGION_FIELD = "gbifRegion";

  private static final int PAGE_SIZE = 20;

  private final ContentCrawlConfiguration configuration;
  private final CDAClient cdaClient;
  private final Client esClient;

  private final CDAContentType contentType;

  /**
   * List of vocabularies ids.
   */
  private final List<String> vocabularies;

  /**
   * ElasticSearch and Contentful configuration are required to create an instance of this class.
   */
  public ContentTypeCrawler(ContentCrawlConfiguration configuration, CDAContentType contentType) {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.elasticSearch, "ElasticSearch configuration can't be null");
    Preconditions.checkNotNull(configuration.contentful, "Contentful configuration can't be null");
    this.configuration = configuration;
    cdaClient = buildCdaClient();
    esClient = buildEsClient(configuration.elasticSearch);
    vocabularies = new ArrayList<>();
    this.contentType = contentType;
  }

  /**
   * Translates a sentence type text into upper camel case format.
   * For example: "Hola Morten" will be transformed into "holaMorten".
   */
  private static String toLowerCamel(String sentence) {
    return CaseFormat.UPPER_UNDERSCORE
            .to(CaseFormat.LOWER_CAMEL, REPLACEMENTS.matcher(sentence).replaceAll("_").toUpperCase());
  }



  private Map<String,Object> getEsSource(CDAArray results) {
    ContentTypeFields contentTypeFields = new ContentTypeFields(contentType);
    results.items().forEach(cdaResource -> {
      if(cdaResource.type() == CDAType.ENTRY) {
        CDAEntry entry = (CDAEntry)cdaResource;
        entry.rawFields().forEach((key,value) -> {
          CDAField field = contentTypeFields.getField(key);
          ContentfulType type = contentTypeFields.getFieldType(key);
          if (ContentfulType.LINK == type) {
            ContentfulLinkType linkType = contentTypeFields.getFieldLinkType(key);
            if (ContentfulLinkType.ASSET == linkType) {

            }
          } else if (ContentfulType.ARRAY == type) {

          }

        });
      }
    });
    return null;
  }
  /*private void nestedElements(CDAArray results, CDAEntry cdaEntry) {
    cdaEntry.contentType().fields().stream()
      .filter(cdaField -> cdaField.type().equals("Link") && cdaField.linkType().equals("Entry"))
      .map(cdaField -> cdaEntry.getField(cdaField.id()))
  }                             */

  /**
   *
   * Returns a Set of the fields that can be get straight from an entry instead of getting its localized version.
   */
  private static Set<String> getCollapsibleFields(CDAContentType contentType) {
    return contentType.fields().stream()
            .filter(cdaField -> COLLAPSIBLE_TYPES.matcher(cdaField.type()).matches())
            .map(CDAField::id).collect(Collectors.toSet());
  }



  /**
   * Extracts the GbifRegion from the isoCountryCode.
   */
  private static  Optional<GbifRegion> getRegion(String isoCountryCode) {
    return Optional.ofNullable(Country.fromIsoCode(isoCountryCode)).map(Country::getGbifRegion);
  }

  /**
   * Iterates trough each Asset entry in cdaEntry and retrieves its value.
   */
  private  Map<String,Map<String,Object>> processAssets(CDAEntry cdaEntry) {
    Map<String,Map<String,Object>> assets = new HashMap<>();
    cdaEntry.rawFields().forEach((field,value) -> {
      Object fieldValue = cdaEntry.getField(field);
      if (CDAAsset.class.isInstance(fieldValue)) {
        assets.put(field, cdaClient.fetch(CDAAsset.class).one(((CDAAsset)fieldValue).id()).rawFields());
      } else if (List.class.isInstance(fieldValue) && (!((List)fieldValue).isEmpty()
                 && CDAAsset.class.isInstance(((List)fieldValue).get(0)))) {
        ((List<?>)fieldValue).forEach(cdaAsset -> assets.put(field, cdaClient.fetch(CDAAsset.class).one(((CDAAsset)cdaAsset).id()).rawFields()));
      }
    });
    return  assets;
  }






  /**
   * Performs the execution of a ElasticSearch BulkRequest and logs the correspondent results.
   */
  private static void executeBulkRequest(BulkRequestBuilder bulkRequest, String contentTypeId) {
    if (bulkRequest.numberOfActions() > 0) {
      BulkResponse bulkResponse = bulkRequest.get();
      if (bulkResponse.hasFailures()) {
        LOG.error("Error indexing.  First error message: {}", bulkResponse.getItems()[0].getFailureMessage());
      } else {
        LOG.info("Indexed [{}] documents of content type [{}]", bulkResponse.getItems().length, contentTypeId);
      }
    } else  {
      LOG.info("Nothing to index for content type [{}]", contentTypeId);
    }
  }

  /**
   * Creates a new instance of a Contentful CDAClient.
   */
  private CDAClient buildCdaClient() {
    return CDAClient.builder().setSpace(configuration.contentful.spaceId)
      .setToken(configuration.contentful.cdaToken).build();
  }


  /**
   * CDAArray that holds references to the list of content types defined in configuration.contentful.contentTypes.
   * If configuration.contentful.contentTypes is empty, all content types are returned.
   */
  private CDAArray getContentTypes() {
    return cdaClient.fetch(CDAContentType.class).all();
  }

}
