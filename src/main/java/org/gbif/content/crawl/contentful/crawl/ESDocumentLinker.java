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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.ScriptSource;


import co.elastic.clients.json.JsonData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.contentful.java.cda.CDAEntry;

import static org.gbif.content.crawl.es.ElasticSearchUtils.getEsIdxName;

/**
 * Utility class to create links between content elements in the ES Document.
 */
public class ESDocumentLinker {

  private static final Logger LOG = LoggerFactory.getLogger(ESDocumentLinker.class);

  private static final String NEWS_UPDATE_SCRIPT  = "if (ctx._source.%1$s == null) "  //field doesn't exist
                                                    + "{ctx._source.%1$s = [params.tag]} " //create new list/array
                                                    + "else if (ctx._source.%1$s.contains(params.tag)) " //value exists
                                                    + "{ ctx.op = \"none\"  } " //do nothing
                                                    + "else { ctx._source.%1$s.add(params.tag) }"; //add new value

  private final String targetContentTypeId;

  private final ElasticsearchClient esClient;

  public ESDocumentLinker(String targetContentTypeId, ElasticsearchClient esClient) {
    this.esClient = esClient;
    this.targetContentTypeId =  targetContentTypeId;
  }

  /**
   * This method updates the news index by adding a new field [contentTypeName]Tag which stores all the ids
   * related to that item from this content type, it is used for creating RSS feeds for specific elements.
   */
  private void processEntryTag(CDAEntry cdaEntry, String esTypeName, String tagValue) {
    if (cdaEntry.contentType().id().equals(targetContentTypeId)) {
      insertTag(cdaEntry, esTypeName, tagValue);
    }
  }

  /**
   * Processes the item associated to an entry.
   * Accepts list of localized resources and a single CDAEntry.
   */
  public void processEntryTag(Object entry, String esTypeName, String tagValue) {
    if (entry instanceof Collection) {
      processEntryTag((Collection<?>)entry, esTypeName, tagValue);
    } else {
      processEntryTag((CDAEntry)entry, esTypeName, tagValue);
    }
  }

  /**
   * Processes a list of possible entries.
   */
  private void processEntryTag(Collection<?> resources, String esTypeName, String tagValue) {
    resources.stream()
      .filter(resource -> resource instanceof CDAEntry
                          && ((CDAEntry) resource).contentType().id().equals(targetContentTypeId))
      .forEach(cdaEntry -> insertTag((CDAEntry) cdaEntry, esTypeName, tagValue));
  }

  /**
   * Inserts the tag in the News index.
   */
  private void insertTag(CDAEntry cdaEntry, String esTypeName, String tagValue) {
    try {
      String indexName = getEsIdxName(cdaEntry.contentType().name());
      String documentId = cdaEntry.id();
      String fieldName = esTypeName + "Tag";
      String scriptSource = String.format(NEWS_UPDATE_SCRIPT, fieldName);

      Map<String, JsonData> params = new HashMap<>();
      params.put("tag", JsonData.of(tagValue));

      Script script = Script.of(s -> s
          .lang("painless")
          .source(ScriptSource.of( sc -> sc.scriptString(scriptSource)))
          .params(params)
      );

      UpdateRequest<Object, Object> updateRequest = UpdateRequest.of(u -> u
          .index(indexName)
          .id(documentId)
          .script(script)
          .retryOnConflict(3)
      );

      esClient.update(updateRequest, Object.class);

      LOG.info("Updated tag {} for entry {} in index {}", tagValue, documentId, indexName);

    } catch (Exception ex) {
      LOG.error("Error updating news tag {} from entry {} ", tagValue, cdaEntry.id(), ex);
    }
  }
}
