package org.gbif.content.crawl.contentful;

import java.util.Map;
import java.util.stream.Collectors;

import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAField;

public class ContentTypeFields {


  private final CDAContentType cdaContentType;

  private final Map<String,Integer> fieldIdx;

  private final Map<String,ContentfulType> fieldType;

  private final Map<String,ContentfulLinkType> fieldLinkType;

  public ContentTypeFields(CDAContentType cdaContentType) {
    this.cdaContentType = cdaContentType;
    fieldIdx = cdaContentType.fields().stream()
                  .collect(Collectors.toMap(CDAField::id, cdaField -> cdaContentType.fields().indexOf(cdaField)));

    fieldType = cdaContentType.fields().stream()
      .collect(Collectors.toMap(CDAField::id, cdaField -> ContentfulType.typeOf(cdaField.type()).get()));

    fieldLinkType = cdaContentType.fields().stream().filter(cdaField -> cdaField.linkType() != null)
      .collect(Collectors.toMap(CDAField::id, cdaField -> ContentfulLinkType.typeOf(cdaField.type()).get()));
  }

  public CDAField getField(String fieldName) {
    return cdaContentType.fields().get(fieldIdx.get(fieldName));
  }

  public ContentfulType getFieldType(String fieldName) {
    return fieldType.get(fieldName);
  }

  public ContentfulLinkType getFieldLinkType(String fieldName) {
    return fieldLinkType.get(fieldName);
  }
}
