package org.gbif.content.crawl.contentful;

import java.util.Arrays;
import java.util.Optional;

/**
 * Know Contentful types.
 */
public enum ContentfulType {

  SYMBOL("Symbol"), TEXT("Text"), BOOLEAN("Boolean"), DATE("Date"), OBJECT("Object"), LOCATION("Location"),
  GEO_POINT("geo_point"), INTEGER("Integer"), DECIMAL("Decimal"), ARRAY("Array"), LINK("Link");

  private final String typeName;

  /**
   * Maps the literal to the type name in Contenful.
   */
  ContentfulType(String typeName) {
    this.typeName = typeName;
  }

  /**
   * Contentful type name.
   */
  public String getTypeName() {
    return typeName;
  }

  /**
   * Gets the literal associated to Contentful type name.
   */
  public static Optional<ContentfulType> typeOf(String type) {
   return Arrays.stream(ContentfulType.values())
            .filter(contentfulType -> contentfulType.getTypeName().equals(type))
            .findFirst();
  }


}
