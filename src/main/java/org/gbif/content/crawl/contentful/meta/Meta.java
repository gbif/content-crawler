package org.gbif.content.crawl.contentful.meta;

import java.util.Map;
import java.util.Optional;

import com.contentful.java.cda.CDAEntry;

/**
 * This class represents the metadata created when a resource was imported from Drupal.
 */
public class Meta {

  //Field names
  private static final String META_FIELD = "meta";
  private static final String DRUPAL_FIELD = "drupal";
  private static final String CREATED_FIELD = "created";


  /**
   * Extracts the Meta information from the CDAEntry.
   */
  public static Optional<String> getMetaCreatedDate(CDAEntry cdaEntry) {
    return Optional.ofNullable(cdaEntry.getField(META_FIELD))
              .map(metaValue -> {
                Map<String,Object> metaValueMap =  (Map<String,Object>)metaValue;
                return (String)((Map<String,Object>)(metaValueMap.get(DRUPAL_FIELD))).get(CREATED_FIELD);
              });
  }

}
