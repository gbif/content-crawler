package org.gbif.content.crawl.contentful;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import com.google.common.collect.Sets;
import io.reactivex.Observable;

/**
 * Utility class that loads contentful vocabularies into memory (i.e.: Map).
 */
public class VocabularyLoader {

  //Know names of fields for vocabulary terms
  private static final Set<String> TERMS_FIELDS = Sets.newHashSet("term", "isoCode");

  private static final int VOC_PAGE_SIZE = 50;

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
      StreamSupport.stream( new ContentfulPager(cdaClient, VOC_PAGE_SIZE, contentTypeId).spliterator(), false)
        .forEach(cdaArray -> cdaArray.items().forEach(cdaResource ->  terms.put(cdaResource.id(),
                                          getFirstTerm(((CDAEntry)cdaResource).rawFields()))));
      return terms;
    });
  }

  /**
   * Gets the first vocabulary fields values tha match with the TERM_FIELDS.
   */
  private static Map<String,String> getFirstTerm(Map<String,Object> values) {
    return (Map<String,String>)TERMS_FIELDS.stream()
              .filter(field -> values.containsKey(field)).findFirst().map(term -> values.get(term)).get();
  }
}
