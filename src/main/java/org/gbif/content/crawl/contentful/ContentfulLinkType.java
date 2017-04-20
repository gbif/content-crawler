package org.gbif.content.crawl.contentful;

import java.util.Arrays;
import java.util.Optional;

/**
 * Contentful Link types.
 */
public enum ContentfulLinkType {
  ASSET("Asset"), ENTRY("Entry"), SPACE("Space");

  private final String linkType;

  /**
   * Maps a Contentful link type name to a literal value.
   */
  ContentfulLinkType(String linkType) {
    this.linkType = linkType;
  }

  /**
   * Contentful link type name.
   */
  public String getLinkType() {
    return linkType;
  }

  /**
   * Literal value mapped to a Contentful link type name to a literal value.
   */
  public static Optional<ContentfulLinkType> typeOf(String linkType) {
    return Arrays.stream(ContentfulLinkType.values())
      .filter(contentfulLinkType -> contentfulLinkType.linkType.equals(linkType))
      .findFirst();
  }
}
