package org.gbif.content.crawl.contentful.crawl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAField;
import com.contentful.java.cma.Constants.CMAFieldType;

import static org.gbif.content.crawl.contentful.crawl.MappingGenerator.COLLAPSIBLE_FIELDS;
import static org.gbif.content.crawl.contentful.crawl.MappingGenerator.COLLAPSIBLE_TYPES;

/**
 * This class creates a cache of common lookup operations on content type fields.
 * It's used to avoid recurrent iterations over fields to extract data used during indexing.
 */
public class ContentTypeFields {

  //Map cache of all type mappings per content type id
  private static final Map<String, ContentTypeFields> cache = new HashMap<>();

  //CDAContent type to be analyzed
  private final CDAContentType cdaContentType;

  //Map from fieldName to idx
  private final Map<String,Integer> fieldIdx;

  //Maps field names to types
  private final Map<String,CMAFieldType> fieldType;

  //Maps field names to link types
  private final Map<String,ContentfulLinkType> fieldLinkType;

  private final Set<String> collapsibleFields;

  /**
   * Initializes all maps and local variables.
   */

  private ContentTypeFields(CDAContentType cdaContentType) {
    this.cdaContentType = cdaContentType;
    fieldIdx = cdaContentType.fields().stream()
      .collect(Collectors.toMap(CDAField::id, cdaField -> cdaContentType.fields().indexOf(cdaField)));

    fieldType = cdaContentType.fields().stream()
      .collect(Collectors.toMap(CDAField::id, cdaField -> CMAFieldType.valueOf(cdaField.type())));

    fieldLinkType = cdaContentType.fields().stream().filter(cdaField -> cdaField.linkType() != null)
      .collect(Collectors.toMap(CDAField::id, cdaField -> ContentfulLinkType.valueOf(cdaField.linkType())));

    collapsibleFields = cdaContentType.fields().stream()
                          .filter(cdaField -> COLLAPSIBLE_TYPES.contains(cdaField.type())
                                              || COLLAPSIBLE_FIELDS.matcher(cdaField.id()).matches())
                          .map(CDAField::id).collect(Collectors.toSet());

  }


  /**
   * Returns the CDAField of a field.
   */
  public CDAField getField(String fieldName) {
    return cdaContentType.fields().get(fieldIdx.get(fieldName));
  }

  /**
   * Returns the ContentfulType of a field.
   */
  public CMAFieldType getFieldType(String fieldName) {
    return fieldType.get(fieldName);
  }

  /**
   * Returns the ContentfulLinkType Type of a field.
   */
  public ContentfulLinkType getFieldLinkType(String fieldName) {
    return fieldLinkType.get(fieldName);
  }

  /**
   * Cans this field be a single value?.
   */
  public boolean isCollapsible(String field) {
    return collapsibleFields.contains(field);
  }

  /**
   * Gets a ContentTypeFields of CDAContentType.
   */
  public static ContentTypeFields of(CDAContentType cdaContentType) {
    return Optional.ofNullable(cache.get(cdaContentType.id()))
            .orElseGet(() ->
              cache.computeIfAbsent(cdaContentType.id(), key -> new ContentTypeFields(cdaContentType))
            );
  }

}
