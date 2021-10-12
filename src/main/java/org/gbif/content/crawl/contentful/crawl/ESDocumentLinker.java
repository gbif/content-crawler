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
import java.util.Collections;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
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

  private final RestHighLevelClient esClient;

  private final String esTargetIndexType;

  public ESDocumentLinker(String targetContentTypeId, RestHighLevelClient esClient, String esTargetIndexType) {
    this.esClient = esClient;
    this.targetContentTypeId =  targetContentTypeId;
    this.esTargetIndexType = esTargetIndexType;
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
    if (Collection.class.isInstance(entry)) {
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
      .filter(resource -> CDAEntry.class.isInstance(resource)
                          && ((CDAEntry) resource).contentType().id().equals(targetContentTypeId))
      .forEach(cdaEntry -> insertTag((CDAEntry) cdaEntry, esTypeName, tagValue));
  }

  /**
   * Inserts the tag in the News index.
   */
  private void insertTag(CDAEntry cdaEntry, String esTypeName, String tagValue) {
    try {
      Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                                 String.format(NEWS_UPDATE_SCRIPT, esTypeName + "Tag"),
                                 Collections.singletonMap("tag", tagValue));
      UpdateRequest updateRequest = new UpdateRequest()
                                      .index(getEsIdxName(cdaEntry.contentType().name()))
                                      .id(cdaEntry.id())
                                      .script(script);
      esClient.update(updateRequest, RequestOptions.DEFAULT);
    } catch (Exception ex) {
      LOG.error("Error updating news tag {} from entry {} ", tagValue, cdaEntry, ex);
    }
  }
}
