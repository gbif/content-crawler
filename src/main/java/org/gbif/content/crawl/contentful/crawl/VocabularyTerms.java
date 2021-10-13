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

  /**
   * Gets the term field of vocabulary content type.
   */
  private static String getVocabularyField(CMAContentType contentType, String fieldName) {
    return contentType.getFields().stream().filter(cdaField -> fieldName.equals(cdaField.getId()))
      .findFirst().map(CMAField::getId)
      .orElseThrow(() -> new IllegalStateException(String.format("ContentType %s is not a vocabulary", contentType.getName())));
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
    return cdaEntry.contentType().id().equals(countryContentTypeId) ? Optional.of(ISO_CODE_FIELD) : Optional.empty();
  }
}
