package org.gbif.content.crawl.contentful;

import org.gbif.content.crawl.es.ElasticSearchUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  private static final String NEWS_UPDATE_SCRIPT  = "if (ctx._source.%1$s == null) {ctx._source.%1$s = [params.tag]} else if (ctx._source.%1$s.contains(params.tag)) { ctx.op = \"none\"  } else { ctx._source.%1$s.add(params.tag) }";

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
  public void processNewsTag(CDAEntry cdaEntry, String esTypeName, String tagValue) {
    if (cdaEntry != null && cdaEntry.contentType().id().equals(newsContentTypeId)) {
      insertTag(cdaEntry, esTypeName, tagValue);
    }
  }

  /**
   * Processes a list of possible news entries
   */
  public void processNewsTag(List<LocalizedResource> resources, String esTypeName, String tagValue) {
    if(resources != null) {
      resources.stream()
        .filter(resource -> CDAEntry.class.isInstance(resource)
                            && ((CDAEntry) resource).contentType().id().equals(newsContentTypeId))
        .forEach(cdaEntry -> insertTag((CDAEntry) cdaEntry, esTypeName, tagValue));
    }
  }

  /**
   * Inserts the tag in the News index.
   */
  private void insertTag(CDAEntry cdaEntry, String esTypeName, String tagValue) {
    try {
      Map<String, Object> params = new HashMap<>();
      params.put("tag", tagValue);
      String scriptText = String.format(NEWS_UPDATE_SCRIPT, esTypeName + "Tag");
      Script script = new Script(ScriptType.INLINE, "painless", scriptText, params);
      esClient.prepareUpdate(ElasticSearchUtils.getEsIdxName(cdaEntry.contentType().name()), esNewsIndexType, cdaEntry.id()).setScript(script).get();
    } catch (Exception ex) {
      LOG.error("Error updating news tag {} from entry {} ", tagValue, cdaEntry, ex);
    }
  }
}
