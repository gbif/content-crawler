package org.gbif.content.crawl.contentful.crawl;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.GbifRegion;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.contentful.java.cda.CDAEntry;

/**
 * Utility class to accumulate values from vocabularies.
 */
public class VocabularyBuilder {

  //Variable to store vocabulary values
  private final Set<String> values;

  //Collects derived GbifRegions from country vocabularies
  private final Set<GbifRegion> gbifRegions;

  private final VocabularyTerms vocabularyTerms;

  /**
   * Constructor. Requires the list of vocabularies content type ids and the content type if of country voc.
   */
  public VocabularyBuilder(VocabularyTerms vocabularyTerms) {
    //Initialize values
    values = new HashSet<>();
    gbifRegions = EnumSet.noneOf(GbifRegion.class);
    this.vocabularyTerms = vocabularyTerms;
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
  public VocabularyBuilder one(Consumer<String> consumer) {
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
  public VocabularyBuilder gbifRegion(Consumer<GbifRegion> consumer) {
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
  public VocabularyBuilder allGbifRegions(Consumer<Set<GbifRegion>> consumer) {
    allGbifRegions().ifPresent(consumer);
    return this;
  }

  /**
   * All collected values.
   */
  public Optional<Set<String>> all() {
    return values.isEmpty() ? Optional.empty() : Optional.of(values);
  }

  /**
   * Consumes all collected values.
   */
  public VocabularyBuilder all(Consumer<Set<String>> consumer) {
    all().ifPresent(consumer);
    return this;
  }

  /**
   * Accumulates vocabularies from a CDAEntry.
   */
  public VocabularyBuilder of(CDAEntry resource) {
    Optional.ofNullable(resource).ifPresent(cdaEntry -> {
      //tries to load a country vocabulary
      vocabularyTerms.countryCodeFieldOf(cdaEntry)
        .map(countryCodeField -> (String)cdaEntry.getField(countryCodeField))
        .ifPresent(countryCode -> {
          values.add(countryCode);
          Optional.ofNullable(Country.fromIsoCode(countryCode).getGbifRegion()).ifPresent(gbifRegions::add);
        });
      //tries to load a vocabulary
      vocabularyTerms.termOf(cdaEntry).map(cdaEntry::getField).ifPresent(vocValue -> values.add((String)vocValue));
    });
    return this;
  }

  /**
   * Accumulates vocabularies from a list of CDAEntry.
   */
  public VocabularyBuilder ofList(Collection<?> resources) {
    resources.stream().filter(CDAEntry.class::isInstance).forEach(resource -> of((CDAEntry)resource));
    return this;
  }
}
