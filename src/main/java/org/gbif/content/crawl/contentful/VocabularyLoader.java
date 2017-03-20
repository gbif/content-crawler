package org.gbif.content.crawl.contentful;

import java.util.HashMap;
import java.util.Map;

import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import io.reactivex.Observable;

/**
 * Utility class that loads contentful vocabularies into memory (i.e.: Map).
 */
public class VocabularyLoader {

  //Know names of fields for vocabulary terms
  public static final String TERM_FIELD = "term";

  /**
   * Private constructor of utility class.
   */
  private VocabularyLoader() {
    //NOP
  }

  /**
   * Loads the terms of a content type.
   * The output structure is:
   *  vocabularyName -> { contentId -> {locale -> termLocalizedValue} }
   */
  public static Observable<Map<String, Map<String,String>>> vocabularyTerms(String contentTypeId,
                                                                                CDAClient cdaClient) {
    return Observable.fromCallable( () -> {
      Map<String, Map<String,String>> terms = new HashMap<>();
      cdaClient.fetch(CDAEntry.class).withContentType(contentTypeId).all().items()
        .forEach(cdaResource -> terms.put(cdaResource.id(),
                                          (Map<String,String>)((CDAEntry)cdaResource).rawFields().get(TERM_FIELD)));
      return terms;
    });
  }
}
