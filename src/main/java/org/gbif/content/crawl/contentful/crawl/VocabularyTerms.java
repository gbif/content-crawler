package org.gbif.content.crawl.contentful.crawl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAField;

/**
 * This utility class keeps map of the field name of content type that contains the descriptive field.
 */
public class VocabularyTerms {

  //All vocabularies should use the term field as the descriptive field
  public static final String TERM_FIELD = "term";

  //The country vocabulary must
  public static final String ISO_CODE_FIELD = "isoCode";

  private static final String ERROR_MSG = "ContentType %s is not a vocabulary";

  //Maps a vocabulary content id to its field that contains the term value
  private final Map<String,String> cache = new HashMap<>();

  private String countryContentTypeId;

  /**
   * Loads the content type vocabulary mapping.
   */
  public void loadVocabulary(CMAContentType contentType) {
    String fieldName = getVocabularyField(contentType, TERM_FIELD);
    cache.put(contentType.getResourceId(), fieldName);
  }

  /**
   * Loads the country content type vocabulary mapping.
   */
  public void loadCountryVocabulary(CMAContentType contentType) {
    String fieldName = getVocabularyField(contentType, ISO_CODE_FIELD);
    countryContentTypeId = contentType.getResourceId();
    cache.put(countryContentTypeId, fieldName);
  }

  private static String getVocabularyField(CMAContentType contentType, String fieldName) {
    return contentType.getFields().stream().filter(cdaField -> fieldName.equals(cdaField.getId()))
      .findFirst().map(CMAField::getId)
      .orElseThrow(() -> new IllegalStateException(String.format(ERROR_MSG, contentType.getName())));
  }

  /**
   * Extract the vocabulary content.
   */
  public Optional<String> termOf(CDAEntry cdaEntry) {
    return Optional.ofNullable(cache.get(cdaEntry.contentType().id()));
  }

  /**
   * Extract the vocabulary content.
   */
  public Optional<String> countryCodeFieldOf(CDAEntry cdaEntry) {
    return cdaEntry.contentType().id().equals(countryContentTypeId)? Optional.of(ISO_CODE_FIELD) :Optional.empty();
  }
}
