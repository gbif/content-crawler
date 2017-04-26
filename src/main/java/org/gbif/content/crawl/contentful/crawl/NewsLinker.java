package org.gbif.content.crawl.contentful.crawl;

import static org.gbif.content.crawl.es.ElasticSearchUtils.getEsIdxName;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.LocalizedResource;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to create links between content elements into the News content type.
 */
public class NewsLinker {

  private static final Logger LOG = LoggerFactory.getLogger(NewsLinker.class);

  private static final String NEWS_UPDATE_SCRIPT  = "if (ctx._source.%1$s == null) "  //field doesn't exist
                                                    + "{ctx._source.%1$s = [params.tag]} " //create new list/array
                                                    + "else if (ctx._source.%1$s.contains(params.tag)) " //value exists
                                                    + "{ ctx.op = \"none\"  } " //do nothing
                                                    + "else { ctx._source.%1$s.add(params.tag) }"; //add new value

  private final String newsContentTypeId;

  private final Client esClient;

  private final String esNewsIndexType;

  public NewsLinker(String newsContentTypeId, Client esClient, String esNewsIndexType) {
    this.esClient = esClient;
    this.newsContentTypeId =  newsContentTypeId;
    this.esNewsIndexType = esNewsIndexType;
  }

  /**
   * Processes the news associated to an entry.
   * This method updates the news index by adding a new field [contentTypeName]Tag which stores all the ids
   * related to that news from this content type, it is used for creating RSS feeds for specific elements.
   */
  private void processNewsTag(CDAEntry cdaEntry, String esTypeName, String tagValue) {
    if (cdaEntry.contentType().id().equals(newsContentTypeId)) {
      insertTag(cdaEntry, esTypeName, tagValue);
    }
  }

  /**
   * Processes the news associated to an entry.
   * Accepts list of localized resources and a single CDAEntry.
   */
  public void processNewsTag(Object entry, String esTypeName, String tagValue) {
    if (Collection.class.isInstance(entry)) {
      processNewsTag((Collection<LocalizedResource>)entry, esTypeName, tagValue);
    } else {
      processNewsTag((CDAEntry)entry, esTypeName, tagValue);
    }
  }

  /**
   * Processes a list of possible news entries.
   */
  private void processNewsTag(Collection<LocalizedResource> resources, String esTypeName, String tagValue) {
    resources.stream()
      .filter(resource -> CDAEntry.class.isInstance(resource)
                          && ((CDAEntry) resource).contentType().id().equals(newsContentTypeId))
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
      esClient.prepareUpdate(getEsIdxName(cdaEntry.contentType().name()),
                             esNewsIndexType, cdaEntry.id()).setScript(script).get();
    } catch (Exception ex) {
      LOG.error("Error updating news tag {} from entry {} ", tagValue, cdaEntry, ex);
    }
  }
}
