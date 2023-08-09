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
package org.gbif.content.crawl.contentful.crawl;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cma.model.CMAContentType;

import io.reactivex.Observable;

import static org.gbif.content.crawl.es.ElasticSearchUtils.createIndex;
import static org.gbif.content.crawl.es.ElasticSearchUtils.getEsIdxName;
import static org.gbif.content.crawl.es.ElasticSearchUtils.getEsIndexingIdxName;
import static org.gbif.content.crawl.es.ElasticSearchUtils.swapIndexToAlias;
import static org.gbif.content.crawl.es.ElasticSearchUtils.toFieldNameFormat;

/**
 * Crawls a single Contentful content type.
 */
public class ContentTypeCrawler {

  //Buffer to use in Observables to accumulate results before send them to ElasticSearch
  private static final int CRAWL_BUFFER = 10;

  private static final Logger LOG = LoggerFactory.getLogger(ContentTypeCrawler.class);

  private static final String CONTENT_TYPE_FIELD = "contentType";

  private static final int PAGE_SIZE = 20;

  private static final TimeValue BULK_REQUEST_TO = TimeValue.timeValueMinutes(5);

  private final CMAContentType contentType;

  private final String esIdxName;

  private final String esIdxAlias;

  private final String esTypeName;

  //Linkers decorate existing entries in the index with supplementary information (like tags)
  private final ESDocumentLinker newsLinker;
  private final ESDocumentLinker articleLinker;

  private final ProgrammeLinker programmeLinker;

  private final MappingGenerator mappingGenerator;
  private final RestHighLevelClient esClient;
  private final CDAClient cdaClient;
  private final VocabularyTerms vocabularyTerms;
  private final ContentCrawlConfiguration.IndexBuild indexConfig;


  ContentTypeCrawler(CMAContentType contentType,
                     MappingGenerator mappingGenerator,
                     RestHighLevelClient esClient,
                     CDAClient cdaClient,
                     VocabularyTerms vocabularyTerms,
                     String newsContentTypeId,
                     String articleContentTypeId,
                     String projectContentTypeId,
                     ContentCrawlConfiguration.IndexBuild indexConfig) {
    this.contentType = contentType;
    //index name has to be in lowercase
    esIdxName = getEsIndexingIdxName(contentType.getName());
    //Index alias
    esIdxAlias = getEsIdxName(contentType.getName());
    //ES type name for this content typ
    esTypeName = toFieldNameFormat(contentType.getName());
    //Used to create links in the indexes
    newsLinker = new ESDocumentLinker(newsContentTypeId, esClient);
    articleLinker = new ESDocumentLinker(articleContentTypeId, esClient);
    programmeLinker = new ProgrammeLinker(projectContentTypeId);

    //Set the mapping generator
    this.mappingGenerator = mappingGenerator;

    this.esClient = esClient;

    this.cdaClient = cdaClient;

    this.vocabularyTerms = vocabularyTerms;

    this.indexConfig = indexConfig;
  }

  /**
   * Crawls the assigned content type into ElasticSearch.
   */
  public void crawl() {
    //gets or (re)create the ES idx if doesn't exists
    createIndex(esClient, esIdxName, mappingGenerator.getEsMapping(contentType));
    LOG.info("Indexing ContentType [{}] into ES Index [{}]", contentType.getName(), esIdxName);
    //Prepares the bulk/batch request
    BulkRequest bulkRequest = new BulkRequest();
    //Retrieves resources in a CDAArray
    Observable.fromIterable(new ContentfulPager(cdaClient, PAGE_SIZE, contentType.getId()))
      .doOnError(err -> { LOG.error("Error crawling content type", err);
                          throw new RuntimeException(err);
                        })
      .buffer(CRAWL_BUFFER)
      .doOnComplete(() -> {
         if(executeBulkRequest(bulkRequest)) {
           swapIndexToAlias(esClient, esIdxAlias, esIdxName, indexConfig);
         }
      })
      .subscribe( results -> results.forEach(
                              cdaArray -> cdaArray.items()
                              .forEach(cdaResource ->
                                         bulkRequest.add(new IndexRequest().index(esIdxName)
                                                           .id(cdaResource.id())
                                                           .source(getESDoc((CDAEntry)cdaResource)))))
      );
  }

  /**
   * Extracts the fields that will be indexed in ElasticSearch.
   */
  private Map<String,Object> getESDoc(CDAEntry cdaEntry) {
    EsDocBuilder esDocBuilder = new EsDocBuilder(cdaEntry, vocabularyTerms,
            nestedCdaEntry -> {
              // decorate any entries, linkers are responsible for filtering suitable content types
              newsLinker.processEntryTag(nestedCdaEntry, esTypeName, cdaEntry.id());
              articleLinker.processEntryTag(nestedCdaEntry, esTypeName, cdaEntry.id());
            });
    programmeLinker.collectProgrammeAcronym(cdaEntry);
    //Add all rawFields
    Map<String, Object> indexedFields =  new HashMap<>(esDocBuilder.toEsDoc());
    indexedFields.put(CONTENT_TYPE_FIELD, esTypeName);
    Optional.ofNullable(programmeLinker.getProgrammeAcronym())
            .ifPresent(programmeAcronym -> indexedFields.put("gbifProgrammeAcronym", programmeAcronym));
    return indexedFields;
  }


  /**
   * Performs the execution of a ElasticSearch BulkRequest and logs the correspondent results.
   */
  private boolean executeBulkRequest(BulkRequest bulkRequest) {
    try {
      if (bulkRequest.numberOfActions() > 0) {
        bulkRequest.timeout(BULK_REQUEST_TO);
        LOG.info("Indexing {} documents into ElasticSearch", bulkRequest.numberOfActions());
        BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
          LOG.error("Error indexing.  First error message: {}", bulkResponse.getItems()[0].getFailureMessage());
          return false;
        } else {
          LOG.info("Indexed [{}] documents of content type [{}]", bulkResponse.getItems().length, esIdxName);
        }
      } else {
        LOG.info("Nothing to index for content type [{}]", esIdxName);
      }
      return true;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
