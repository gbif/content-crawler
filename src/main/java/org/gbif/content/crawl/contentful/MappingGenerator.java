package org.gbif.content.crawl.contentful;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAField;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class MappingGenerator {

  private static final String KEYWORD = "keyword";
  private static final String NESTED = "nested";

  private static final Pattern IGNORED_FIELDS = Pattern.compile("images|space|revision|type");
  private static final Pattern BOOSTED_FIELDS = Pattern.compile("title|description");

  private static final Map<String,String> CONTENTFUL_ES_TYPE_MAP = new ImmutableMap.Builder()
    .put("Symbol", KEYWORD)
    .put("Text", "text")
    .put("Boolean", "boolean")
    .put("Date", "date")
    .put("Object", NESTED)
    .put("Location", "geo_point")
    .put("Integer", "integer")
    .put("Decimal", "double")
    .build();
  private static final String TYPE = "type";
  private static final String ARRAY_TYPE = "Array";
  private static final String ASSET_TYPE = "Asset";
  private static final String LINK = "Link";
  private static final String LINK_TYPE = "linkType";
  private static final String LINK_CONTENT_TYPE = "linkContentType";
  private static final String VALIDATIONS = "validations";


  private final Set<CDAContentType> vocabularies;

  public MappingGenerator(Set<CDAContentType> vocabularies) {
    this.vocabularies = vocabularies;
  }

  public String getEsMapping(CDAContentType contentType) {
    try (XContentBuilder mapping = XContentFactory.jsonBuilder()) {
      mapping.startObject();
        mapping.startObject("_all");
          mapping.field("store", Boolean.TRUE);
        mapping.endObject();
        mapping.startArray("dynamic_templates");
        contentType.fields().stream().filter(cdaField -> !cdaField.isDisabled()).forEach(cdaField -> {
          esType(cdaField).ifPresent(esType -> {
            if (!NESTED.equals(esType)) {
              addTemplateField("path_match", cdaField.id(), cdaField.id() + ".*", esType, mapping);
            } else {
              addTemplateField("match", cdaField.id(), cdaField.id(), esType, mapping);
            }
          });
        });
        mapping.endArray();
      mapping.endObject();
      return mapping.string();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void addTemplateField(String match, String fieldName, String fieldPattern, String esType, XContentBuilder mapping) {
    try {
      mapping.startObject();
        mapping.startObject(fieldName);
          mapping.field(match, fieldPattern);
          mapping.startObject("mapping");
            if (IGNORED_FIELDS.matcher(fieldName).matches()) {
              mapping.field("include_in_all", Boolean.FALSE);
              mapping.field("enabled", Boolean.FALSE);
            } else {
              mapping.field("type", esType);
              if (BOOSTED_FIELDS.matcher(fieldName).matches()) {
                mapping.field("boost", 10);
              }
            }
          mapping.endObject();
        mapping.endObject();
      mapping.endObject();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private Optional<String> esType(CDAField cdaField) {
    if (isLink(cdaField)) {
      return Optional.ofNullable(getEsLinkType(cdaField));
    }
    if (cdaField.type().equals(ARRAY_TYPE)) {
      return Optional.ofNullable(CONTENTFUL_ES_TYPE_MAP.get(cdaField.items().get(TYPE)));
    }
    return Optional.ofNullable(CONTENTFUL_ES_TYPE_MAP.get(cdaField.type()));
  }

  private static boolean isLink(CDAField cdaField) {
    return cdaField.type().equals(LINK) || (cdaField.type().equals(ARRAY_TYPE)
                                             && cdaField.items().get(TYPE).equals(LINK));
  }

  private String getEsLinkType(CDAField cdaField) {
    String linkType = cdaField.linkType();
    Optional<String> linkContentType = cdaField.validations() == null? Optional.empty() :
                                                                      cdaField.validations().stream()
                                                                        .filter(validation -> validation.containsKey(LINK_CONTENT_TYPE))
                                                                        .map(validation -> (String)validation.get(LINK_CONTENT_TYPE))
                                                                        .findFirst();
    if (ARRAY_TYPE.equals(cdaField.type())) {
      linkType = (String)cdaField.items().get(LINK_TYPE);
      linkContentType = ((List<Map<String,Object>>)cdaField.items().get(VALIDATIONS)).stream()
                          .map(validation -> ((List<String>)validation.get(LINK_CONTENT_TYPE)).get(0)).findFirst();
    }
    Optional<String> fLinkType = linkContentType;
    return (fLinkType.isPresent()
            && vocabularies.stream().anyMatch(cdaContentType -> cdaContentType.id().equals(fLinkType.get())))? KEYWORD : NESTED;
  }

}
