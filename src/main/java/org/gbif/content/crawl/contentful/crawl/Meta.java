package org.gbif.content.crawl.contentful.crawl;

import java.util.Map;
import java.util.Optional;

import com.contentful.java.cda.CDAEntry;

/**
 * This class represents the metadata created when a resource was imported from Drupal.
 */
public class Meta {

  //Known field names
  private static final String META_FIELD = "meta";
  private static final String DRUPAL_FIELD = "drupal";
  private static final String CREATED_FIELD = "created";

  /**
   * Private constructor.
   */
  private Meta() {
    //NOP
  }

  /**
   * Extracts the Meta information from the CDAEntry.
   */
  public static Optional<String> getMetaCreatedDate(CDAEntry cdaEntry) {
    return Optional.ofNullable(cdaEntry.getField(META_FIELD))
              .map(metaValue ->
                (String)((Map<String,Object>)((Map<String,Object>)metaValue).get(DRUPAL_FIELD)).get(CREATED_FIELD)
              );
  }

}
