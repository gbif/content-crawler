package org.gbif.content.crawl.contentful;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.GbifRegion;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.contentful.java.cda.CDAEntry;
import com.google.common.collect.Sets;

/**
 * Utility class to accumulate values from vocabularies.
 */
public class VocabularyCollector {

  //Know names of fields for vocabulary terms
  public static final Set<String> TERMS_FIELDS = Sets.newHashSet("term", "isoCode");

  //Variable to store vocabulary values
  private Set<String> values;

  //Collects derived GbifRegions from country vocabularies
  private Set<GbifRegion> gbifRegions;

  //List of vocabularies content type ids
  private final Set<String> vocabulariesContentTypeIds;

  //Id of country content type id
  private final String countryContentTypeId;

  /**
   * Constructor. Requires the list of vocabularies content type ids and the content type if of country voc.
   */
  public VocabularyCollector(Set<String> vocabulariesContentTypeIds, String countryContentTypeId) {
    this.vocabulariesContentTypeIds = vocabulariesContentTypeIds;
    this.countryContentTypeId = countryContentTypeId;
    //Initialize values
    values = new HashSet<>();
    gbifRegions = new HashSet<>();
  }

  /**
   * Assuming that only one value has been collected, returned it.
   */
  public Optional<String> one() {
    return values.stream().findFirst();
  }

  /**
   * Consumes the only value collected.
   */
  public VocabularyCollector one(Consumer<String> consumer) {
    one().ifPresent(consumer);
    return this;
  }

  /**
   * Has any value been collected.
   */
  public boolean isEmpty() {
    return values.isEmpty();
  }

  /**
   * Interpreted GbifRegion from seen values.
   */
  public Optional<GbifRegion> gbifRegion() {
    return gbifRegions.stream().findFirst();
  }

  /**
   * Consumes the GbifRegion value.
   */
  public VocabularyCollector gbifRegion(Consumer<GbifRegion> consumer) {
    gbifRegion().ifPresent(consumer);
    return this;
  }

  /**
   *
   * All the GbifRegion interpreted from seen values.
   */
  public Optional<Set<GbifRegion>> allGbifRegions() {
    return gbifRegions.isEmpty()? Optional.empty() : Optional.of(gbifRegions);
  }

  /**
   * Consumer all the seen GbifRegions.
   */
  public VocabularyCollector allGbifRegions(Consumer<Set<GbifRegion>> consumer) {
    allGbifRegions().ifPresent(consumer);
    return this;
  }

  /**
   * All collected values.
   */
  public Optional<Set<String>> all() {
    return values.isEmpty()? Optional.empty() : Optional.of(values);
  }

  /**
   * Consumes all collected values.
   */
  public VocabularyCollector all(Consumer<Set<String>> consumer) {
    all().ifPresent(consumer);
    return this;
  }

  /**
   * Accumulates vocabularies from a CDAEntry.
   */
  public VocabularyCollector of(CDAEntry cdaEntry) {
    if(cdaEntry != null) {
      extractVocabulary(cdaEntry).ifPresent(vocValue -> {
        values.add(vocValue);
        if (countryContentTypeId.equals(cdaEntry.contentType().id())) {
          Optional.ofNullable(Country.fromIsoCode(vocValue).getGbifRegion()).ifPresent(gbifRegions::add);
        }
      });
    }
    return this;
  }

  /**
   * Accumulates vocabularies from a list of CDAEntry.
   */
  public VocabularyCollector ofList(List<?> resources) {
    resources.stream().filter(CDAEntry.class::isInstance).forEach(resource -> of((CDAEntry)resource));
    return this;
  }

  /**
   * Extract the vocabulary content.
   */
  private Optional<String> extractVocabulary(CDAEntry cdaEntry) {
    return vocabulariesContentTypeIds.contains(cdaEntry.contentType().id()) ? cdaEntry.rawFields()
      .keySet()
      .stream()
      .filter(TERMS_FIELDS::contains)
      .findFirst()
      .map(cdaEntry::getField) : Optional.empty();
  }
}
