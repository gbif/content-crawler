package org.gbif.content.crawl.contentful;

import java.util.Arrays;
import java.util.Optional;

public enum ContentfulType {

  SYMBOL("Symbol"), TEXT("Text"), BOOLEAN("Boolean"), DATE("Date"), OBJECT("Object"), LOCATION("Location"),
  GEO_POINT("geo_point"), INTEGER("Integer"), DECIMAL("Decimal"), ARRAY("Array"), LINK("Link");

  private final String typeName;

  ContentfulType(String typeName) {
    this.typeName = typeName;
  }

  public String getTypeName() {
    return typeName;
  }

  public static Optional<ContentfulType> typeOf(String typeName) {
   return Arrays.stream(ContentfulType.values())
            .filter( contentfulType -> contentfulType.getTypeName().equals(typeName))
            .findFirst();
  }


}
