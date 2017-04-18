package org.gbif.content.crawl.contentful;

import java.util.Arrays;
import java.util.Optional;

public enum ContentfulLinkType {
  ASSET("Asset"), ENTRY("Entry"), SPACE("Space");

  private final String linkType;

  ContentfulLinkType(String linkType) {
    this.linkType = linkType;
  }

  public String getLinkType() {
    return linkType;
  }

  public static Optional<ContentfulLinkType> typeOf(String linkType) {
    return Arrays.stream(ContentfulLinkType.values())
      .filter(contentfulLinkType -> contentfulLinkType.linkType.equals(linkType))
      .findFirst();
  }
}
