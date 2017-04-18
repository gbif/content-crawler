package org.gbif.content.crawl.contentful;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAAsset;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.CDAField;
import com.contentful.java.cda.CDAResource;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import io.reactivex.Observable;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.content.crawl.contentful.MappingGenerator.COLLAPSIBLE_FIELDS;
import static org.gbif.content.crawl.es.ElasticSearchUtils.buildEsClient;
import static org.gbif.content.crawl.es.ElasticSearchUtils.createIndex;
import static org.gbif.content.crawl.contentful.MappingGenerator.COLLAPSIBLE_TYPES;

/**
 * Pulls content from Contentful and stores it in ElasticSearch indexes.
 */
public class ContentfulCrawler {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulCrawler.class);

  private static final Pattern REPLACEMENTS = Pattern.compile(":\\s+|\\s+");

  private static final Pattern LINKED_ENTRY_FIELDS = Pattern.compile(".*summary.*|.*title.*|label|url");

  private static final String CONTENT_TYPE_FIELD = "contentType";
  private static final String REGION_FIELD = "gbifRegion";

  private static final int PAGE_SIZE = 20;

  private final ContentCrawlConfiguration configuration;
  private final CDAClient cdaClient;
  private final Client esClient;

  //Content Type Id
  private String countryVocabularyId;

  /**
   * vocabularyName -> { contentId -> defaultValue} }
   */
  private final Map<String,Map<String, String>> vocabularies;

  /**
   * ElasticSearch and Contentful configuration are required to create an instance of this class.
   */
  public ContentfulCrawler(ContentCrawlConfiguration configuration) {
    Preconditions.checkNotNull(configuration, "Crawler configuration can't be null");
    Preconditions.checkNotNull(configuration.elasticSearch, "ElasticSearch configuration can't be null");
    Preconditions.checkNotNull(configuration.contentful, "Contentful configuration can't be null");
    this.configuration = configuration;
    cdaClient = buildCdaClient();
    esClient = buildEsClient(configuration.elasticSearch);
    vocabularies = new HashMap<>();
  }

  /**
   * Translates a sentence type text into upper camel case format.
   * For example: "Hola Morten" will be transformed into "holaMorten".
   */
  private static String toLowerCamel(String sentence) {
    return CaseFormat.UPPER_UNDERSCORE
            .to(CaseFormat.LOWER_CAMEL, REPLACEMENTS.matcher(sentence).replaceAll("_").toUpperCase());
  }

  /**
   * Executes the Crawler in a synchronous way, i.e.: process each resource per content type sequentially.
   */
  public void run() {
    List<CDAContentType> contentTypes = getContentTypes().items().stream()
                                          .map(cdaResource -> (CDAContentType)cdaResource).collect(Collectors.toList());
    Set<CDAContentType> vocContentTypes = contentTypes.stream()
      .filter(contentType -> configuration.contentful.vocabularies.contains(contentType.name()))
      .collect(Collectors.toSet());
    vocContentTypes.forEach(contentType -> {
      //Keeps the country vocabulary ID for future use
      if (contentType.name().equals(configuration.contentful.countryVocabulary)) {
        countryVocabularyId = contentType.id();
      }
      //Loads vocabulary into memory
      VocabularyLoader.vocabularyTerms(contentType.id(), cdaClient)
        .subscribe(terms -> vocabularies.put(contentType.id(), terms));
    });

    //Mapping generator can be re-used for all content types
    MappingGenerator mappingGenerator = new MappingGenerator(vocContentTypes);

    contentTypes.stream().filter(contentType -> configuration.contentful.contentTypes.contains(contentType.name()))
      .forEach(contentType -> {
        //index name has to be in lowercase
        String idxName = REPLACEMENTS.matcher(contentType.name()).replaceAll("").toLowerCase();
        //gets or (re)create the ES idx if doesn't exists
        createIndex(esClient, configuration.contentful.indexBuild, idxName, mappingGenerator.getEsMapping(contentType));
        LOG.info("Indexing ContentType [{}] into ES Index [{}]", contentType.name(), idxName);
        //Prepares the bulk/batch request
        BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        Set<String> collapsibleFields = getCollapsedFields(contentType);
        //Retrieves resources in a CDAArray
        Observable.fromIterable(new ContentfulPager(cdaClient, PAGE_SIZE, contentType.id()))
          .doOnComplete(() -> executeBulkRequest(bulkRequest, contentType.id()))
          .subscribe(results -> results.items()
            .forEach(cdaResource ->
                       bulkRequest.add(esClient.prepareIndex(idxName.toLowerCase(),
                                                             configuration.contentful.indexBuild.esIndexType,
                                                             cdaResource.id())
                                         .setSource(getIndexedFields((CDAEntry)cdaResource,
                                                                     toLowerCamel(contentType.name()),
                                                                     collapsibleFields)))
            ));
      });
  }

  /**
   * Extracts the fields that will be indexed in ElasticSearch.
   */
  private Map<String,Object> getIndexedFields(CDAEntry cdaEntry, String contentTypeName, Set<String> collapsedFields) {
    //Add all rawFields
    Map<String, Object> indexedFields = fieldsFromEntry(cdaEntry, collapsedFields);
    //Add meta attributes
    indexedFields.putAll(cdaEntry.attrs());
    indexedFields.put(CONTENT_TYPE_FIELD, contentTypeName);
    return indexedFields;
  }


  /**
   * Iterates trough each Asset entry in cdaEntry and retrieves its value.
   */
  private  Map<String,Object> fieldsFromEntry(CDAEntry cdaEntry, Set<String> collapsibleFields) {
    Map<String,Object> entries = new HashMap<>();
    cdaEntry.rawFields().forEach((field,value) -> {
      Object fieldValue = cdaEntry.getField(field);
      if (CDAAsset.class.isInstance(fieldValue)) {
        entries.put(field, ((CDAAsset)fieldValue).rawFields());
      } else if (List.class.isInstance(fieldValue) && (!((List)fieldValue).isEmpty()
                 && CDAAsset.class.isInstance(((List)fieldValue).get(0)))) {
        ((List<?>)fieldValue).forEach(cdaAsset -> entries.put(field, ((CDAAsset)cdaAsset).rawFields()));
      } else if (CDAEntry.class.isInstance(fieldValue)) {
        entries.put(field, getAssociatedEntryFields((CDAEntry)fieldValue));
      } else if (List.class.isInstance(fieldValue) && (!((List)fieldValue).isEmpty()
                                                       && CDAEntry.class.isInstance(((List)fieldValue).get(0)))) {
        if (vocabularies.containsKey(((CDAEntry) ((List) fieldValue).get(0)).contentType().id())) {
          List<String> vocabularyValues  = getVocabularyValues(field, cdaEntry);
          if (countryVocabularyId.equals(((CDAEntry) ((List) fieldValue).get(0)).contentType().id())) {
            Set<GbifRegion> regions = vocabularyValues.stream().map(isoCountryCode -> Country.fromIsoCode(isoCountryCode).getGbifRegion()).collect(Collectors.toSet());
            entries.compute(REGION_FIELD, (k,v) -> v == null? regions: ((Set<GbifRegion>)v).addAll(regions));
          }
          entries.put(field, vocabularyValues);

        } else {
          ((List<?>) fieldValue).forEach(nestedEntry -> entries.put(field, getAssociatedEntryFields((CDAEntry)nestedEntry)));
        }
      } else {
        entries.put(field, value);
      }
      if (collapsibleFields.contains(field)) {
        entries.replace(field, fieldValue);
      }
    });
    return entries;
  }

  /**
   *
   * Returns a Set of the fields that can be get straight from an entry instead of getting its localized version.
   */
  private static Set<String> getCollapsedFields(CDAContentType contentType) {
    return contentType.fields().stream()
      .filter(cdaField -> COLLAPSIBLE_TYPES.matcher(cdaField.type()).matches()
                          || COLLAPSIBLE_FIELDS.matcher(cdaField.id()).matches())
      .map(CDAField::id).collect(Collectors.toSet());
  }

  /**
   * Associated entities are indexed using title, summary and id.
   */
  public static Map<String, Object> getAssociatedEntryFields(CDAEntry cdaEntry) {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("id", cdaEntry.id());
    fields.putAll(cdaEntry.rawFields().entrySet().stream()
                                  .filter(entry -> LINKED_ENTRY_FIELDS.matcher(entry.getKey()).matches())
                                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    return fields;
  }

  /**
   * Extracts the vocabulary term values.
   */
  private List<String> getVocabularyValues(String vocabularyField, CDAEntry cdaEntry) {
    List<CDAEntry> vocabulary = cdaEntry.getField(vocabularyField);
    return vocabulary.stream()
            .map(entry -> vocabularies.get(entry.contentType().id()).get(entry.id()))
            .collect(Collectors.toList());


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
