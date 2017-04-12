package org.gbif.content.crawl.contentful;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.contentful.java.cda.CDAContentType;
import com.contentful.java.cda.CDAField;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class MappingGenerator {

  private static final String KEYWORD = "keyword";
  private static final String NESTED = "nested";
  private static final String VOCABULARY = "vocabulary";

  /**
   * Fields that are stored but not indexed/analized.
   */
  private static final Pattern IGNORED_FIELDS = Pattern.compile("space|revision|type");

  /**
   * List of types that can be obtained using a non-localized version.
   */
  public static final Pattern COLLAPSIBLE_TYPES = Pattern.compile("Boolean|Object");

  /**
   * Fields that are boosted by default.
   */
  private static final Pattern BOOSTED_FIELDS = Pattern.compile("title|description");

  /**
   * Boost value given to BOOSTED_FIELDS.
   */
  private static final int HIGH_BOOST = 10;

  /**
   * Fields that are common to all contentful content types.
   */
  private static final Map<String, String> KNOWN_FIELDS = new ImmutableMap.Builder().put("locale", KEYWORD )
    .put("contentType", KEYWORD)
    .put("id", KEYWORD)
    .put("createdAt", "date")
    .put("updatedAt", "date")
    .put("revision", "float")
    .put("gbifRegion", KEYWORD)
    .put("type", KEYWORD).build();

  /**
   * Mapping of Contentful to ElasticSearch data types.
   */
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

  //Commonly used constants
  private static final String TYPE = "type";
  private static final String ARRAY_TYPE = "Array";
  private static final String ASSET_TYPE = "Asset";
  private static final String LINK = "Link";
  private static final String LINK_CONTENT_TYPE = "linkContentType";
  private static final String VALIDATIONS = "validations";

  /**
   * List of content types that store vocabularies.
   */
  private final Set<CDAContentType> vocabularies;

  /**
   * Creates a Json structure like:
   * "properties": {
   *   "fieldName": {
   *     "type": type
   *   }
   * }
   * All KNOWN_FIELDS are mapped to data type 'keyword'.
   */
  private static void addProperties(XContentBuilder mapping, Map<String, String> collapsedFields) throws IOException {
    Map<String,String> flatFields = new HashMap<>(collapsedFields);
    flatFields.putAll(KNOWN_FIELDS);
    mapping.startObject("properties");
    flatFields.forEach((key,type) -> {
        try {
          mapping.startObject(key);
            mapping.field("type", type);
          mapping.endObject();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
    mapping.endObject();
  }

  /**
   * Produces the following JSON structure:
   * {
   *  "ignored_fields": {
   *    "match": "images|space|revision|type",
   *    "match_pattern": "regex",
   *    "mapping": {
   *      "enabled": false,
   *      "include_in_all": false
   *    }
   *  }
   * }
   */
  private static void addDefaultIgnoredFields(XContentBuilder mapping) throws IOException {
    mapping.startObject();
      mapping.startObject("ignored_fields");
        mapping.field("match", IGNORED_FIELDS.pattern());
        mapping.field("match_pattern", "regex");
        mapping.startObject("mapping");
          mapping.field("enabled", Boolean.FALSE);
          mapping.field("include_in_all", Boolean.FALSE);
        mapping.endObject();
      mapping.endObject();
    mapping.endObject();
  }


  /**
   * Produces the following JSON structure:
   * {
   *  "asset_files": {
   *    "path_match": "*.file.*",
   *    "mapping": {
   *      "enabled": false,
   *      "include_in_all": false
   *    }
   *  }
   * }
   */
  private static void addFileAssetMapping(XContentBuilder mapping) throws IOException {
    mapping.startObject();
      mapping.startObject("asset_files");
        mapping.field("path_match", "*.file.*");
        mapping.startObject("mapping");
          mapping.field("enabled", Boolean.FALSE);
          mapping.field("include_in_all", Boolean.FALSE);
        mapping.endObject();
      mapping.endObject();
    mapping.endObject();
  }

  /**
   * Produces the following JSON structure:
   * {
   *  "asset_fieldName": {
   *    "path_match": "*.fieldName.*",
   *    "mapping": {
   *      "type": esType
   *    }
   *  }
   * }
   */
  private static void addNestedMapping(XContentBuilder mapping, String fieldName, String esType) throws IOException {
    mapping.startObject();
      mapping.startObject("nested_" + fieldName);
        mapping.field("path_match", "*." + fieldName + ".*");
        mapping.startObject("mapping");
          mapping.field("type", esType);
        mapping.endObject();
      mapping.endObject();
    mapping.endObject();
  }

  /**
   * Produces a Json object like:
   * {
   *   "fieldName": {
   *     "match": fieldPattern,
   *     "mapping": {
   *       "type": esType,
   *       "boost": HIGH_BOOST
   *     }
   *   }
   * }
   */
  public static void addTemplateField(String match, String fieldName, String fieldPattern, String esType,
                                      XContentBuilder mapping) {
    try {
      mapping.startObject();
        mapping.startObject(fieldName);
          mapping.field(match, fieldPattern);
          mapping.startObject("mapping");
            mapping.field("type", esType);
            if (BOOSTED_FIELDS.matcher(fieldName).matches()) {
              mapping.field("boost", HIGH_BOOST);
            }
          mapping.endObject();
        mapping.endObject();
      mapping.endObject();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Checks if the CDAField represents a Link to another resource.
   */
  private static boolean isLink(CDAField cdaField) {
    return cdaField.type().equals(LINK) || (cdaField.type().equals(ARRAY_TYPE)
                                             && cdaField.items().get(TYPE).equals(LINK));
  }

  /**
   * Default constructor.
   */
  public MappingGenerator(Set<CDAContentType> vocabularies) {
    this.vocabularies = vocabularies;
  }

  /**
   * Generates the mappings file for ElasticSearch index.
   * In general terms it will produce a structure like:
   * {
   *   "_all": {
   *     "store": true
   *   },
   *   "dynamic_templates": [
   *   {
   *      "ignored_fields": {
   *        "match": "images|space|revision|type",
   *        "match_pattern": "regex",
   *        "mapping": {
   *            "enabled": false,
   *            "include_in_all": false
   *          }
   *        }
   *      }
   *      {
   *        "template1" : {
   *          "match|path_match": fieldPattern, (in general match or path_match are used)
   *          "mapping": {
   *            "type": dataType
   *            "boost": 10   <-- optional
   *          }
   *        }
   *      ...
   *      }
   *   ],
   *   "properties": {
   *      "property1": {
   *        "type": dataType
   *      }
   *      ...
   *   }
   * }
   */
  public String getEsMapping(CDAContentType contentType) {
    Map<String, String> collapsedFields = new HashMap<>();
    try (XContentBuilder mapping = XContentFactory.jsonBuilder()) {
      mapping.startObject();
        mapping.startObject("_all");
          mapping.field("store", Boolean.TRUE);
        mapping.endObject();
        mapping.startArray("dynamic_templates");
        addDefaultIgnoredFields(mapping);
        addFileAssetMapping(mapping);
        addNestedMapping(mapping, "title", KEYWORD);
        addNestedMapping(mapping, "description", "text");
        contentType.fields().stream().filter(cdaField -> !cdaField.isDisabled()).forEach(cdaField ->
          esType(cdaField).ifPresent(esType -> {
            if (VOCABULARY.equals(esType)) {
              collapsedFields.put(cdaField.id(), KEYWORD);
            } else if (COLLAPSIBLE_TYPES.matcher(cdaField.type()).matches()) {
              collapsedFields.put(cdaField.id(), esType);
            } else if (!NESTED.equals(esType)) {
              addTemplateField("path_match", cdaField.id(), cdaField.id() + ".*", esType, mapping);
            } else {
              addTemplateField("match", cdaField.id(), cdaField.id(), esType, mapping);
            }
          })
        );
        mapping.endArray();
        addProperties(mapping, collapsedFields);
      mapping.endObject();
      return mapping.string();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Gets the ElasticSearch deduced type of the CDAField.
   */
  private Optional<String> esType(CDAField cdaField) {
    if (isLink(cdaField)) {
      return Optional.ofNullable(getEsLinkType(cdaField));
    }
    if (cdaField.type().equals(ARRAY_TYPE)) {
      return Optional.ofNullable(CONTENTFUL_ES_TYPE_MAP.get(cdaField.items().get(TYPE)));
    }
    return Optional.ofNullable(CONTENTFUL_ES_TYPE_MAP.get(cdaField.type()));
  }

  /**
   * Gets the ElasticSearch deduced type of a Link.
   */
  private String getEsLinkType(CDAField cdaField) {
    Optional<String> linkContentType = cdaField.validations() == null? Optional.empty() :
                                                                      cdaField.validations().stream()
                                                                        .filter(validation -> validation.containsKey(LINK_CONTENT_TYPE))
                                                                        .map(validation -> (String)validation.get(LINK_CONTENT_TYPE))
                                                                        .findFirst();
    if (ARRAY_TYPE.equals(cdaField.type())) {
      linkContentType = ((List<Map<String,Object>>)cdaField.items().get(VALIDATIONS)).stream()
                          .map(validation -> ((List<String>)validation.get(LINK_CONTENT_TYPE)).get(0)).findFirst();
    }
    Optional<String> fLinkType = linkContentType;
    return (fLinkType.isPresent()
            && vocabularies.stream().anyMatch(cdaContentType -> cdaContentType.id().equals(fLinkType.get())))? VOCABULARY : NESTED;
  }

}
