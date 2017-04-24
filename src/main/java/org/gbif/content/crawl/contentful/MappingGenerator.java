package org.gbif.content.crawl.contentful;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.contentful.java.cma.Constants;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAField;
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
  public static final Set<Constants.CMAFieldType> COLLAPSIBLE_TYPES = EnumSet.of(Constants.CMAFieldType.Boolean);

  /**
   * List of FIELDS that can be obtained using a non-localized version.
   */
  public static final Pattern COLLAPSIBLE_FIELDS = Pattern.compile("meta");

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
  private static final Map<Constants.CMAFieldType,String> CONTENTFUL_ES_TYPE_MAP = new ImmutableMap.Builder()
    .put(Constants.CMAFieldType.Symbol, KEYWORD)
    .put(Constants.CMAFieldType.Text, "text")
    .put(Constants.CMAFieldType.Boolean, "boolean")
    .put(Constants.CMAFieldType.Date, "date")
    .put(Constants.CMAFieldType.Object, NESTED)
    .put(Constants.CMAFieldType.Location, "geo_point")
    .put(Constants.CMAFieldType.Integer, "integer")
    .put(Constants.CMAFieldType.Number, "double")
    .build();

  //Commonly used constants
  private static final String TYPE = "type";
  private static final String LINK_CONTENT_TYPE = "linkContentType";
  private static final String VALIDATIONS = "validations";

  /**
   * List of content types that store vocabularies.
   */
  private final Set<String> vocabularies;

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
   *  "ignored_fields": {
   *    "match": "*Tag",
   *    "match_pattern": "regex",
   *    "mapping": {
   *      "type": "keyword",
   *      "include_in_all": false
   *    }
   *  }
   * }
   *
   * This is used to create links between content types, in particular in the News content type.
   */
  private static void addGenericTagsMapping(XContentBuilder mapping) throws IOException {
    mapping.startObject();
      mapping.startObject("generic_tags");
        mapping.field("match", ".*Tag");
        mapping.field("match_pattern", "regex");
        mapping.startObject("mapping");
          mapping.field("type", KEYWORD);
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
  private static boolean isLink(CMAField cdaField) {
    return cdaField.getType() == Constants.CMAFieldType.Link
           || (cdaField.getType() == Constants.CMAFieldType.Array
               && cdaField.getArrayItems().get(TYPE).equals(Constants.CMAFieldType.Link.name()));
  }

  /**
   * Default constructor.
   */
  public MappingGenerator(Set<CMAContentType> vocabularies) {
    this.vocabularies = vocabularies.stream().map(CMAContentType::getResourceId).collect(Collectors.toSet());
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
  public String getEsMapping(CMAContentType contentType) {
    Map<String, String> collapsedFields = new HashMap<>();
    try (XContentBuilder mapping = XContentFactory.jsonBuilder()) {
      mapping.startObject();
        mapping.startObject("_all");
          mapping.field("store", Boolean.TRUE);
        mapping.endObject();
        mapping.startArray("dynamic_templates");
        addDefaultIgnoredFields(mapping);
        addFileAssetMapping(mapping);
        addGenericTagsMapping(mapping);
        addNestedMapping(mapping, "title", KEYWORD);
        addNestedMapping(mapping, "description", "text");
        contentType.getFields().stream().filter(cmaField -> !cmaField.isDisabled()).forEach(cmaField ->
          esType(cmaField).ifPresent(esType -> {
            if (VOCABULARY.equals(esType)) {
              collapsedFields.put(cmaField.getId(), KEYWORD);
            } else if (COLLAPSIBLE_TYPES.contains(cmaField.getType())
                       || COLLAPSIBLE_FIELDS.matcher(cmaField.getId()).matches()) {
              collapsedFields.put(cmaField.getId(), esType);
            } else if (!NESTED.equals(esType)) {
              addTemplateField("path_match", cmaField.getId(), cmaField.getId() + ".*", esType, mapping);
            } else {
              addTemplateField("match", cmaField.getId(), cmaField.getId(), esType, mapping);
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
  private Optional<String> esType(CMAField cmaField) {
    if (isLink(cmaField)) {
      return Optional.ofNullable(getEsLinkType(cmaField));
    }
    if (cmaField.getType() == Constants.CMAFieldType.Array) {
      return Optional.ofNullable(CONTENTFUL_ES_TYPE_MAP.get(cmaField.getArrayItems().get(TYPE)));
    }
    return Optional.ofNullable(CONTENTFUL_ES_TYPE_MAP.get(cmaField.getType()));
  }

  /**
   * Gets the ElasticSearch deduced type of a Link.
   */
  private String getEsLinkType(CMAField cmaField) {
    Optional<String> linkType = Optional.ofNullable(cmaField.getLinkType());
    Optional<String> linkContentType = linkType.isPresent() ?
       cmaField.getValidations().stream().filter(validation -> validation.containsKey(LINK_CONTENT_TYPE))
         .map(validation -> ((List<String>)validation.get(LINK_CONTENT_TYPE)).get(0)).findFirst()
      : ((List<Map<String,Object>>)cmaField.getArrayItems().get(VALIDATIONS)).stream()
        .map(validation -> ((List<String>)validation.get(LINK_CONTENT_TYPE)).get(0)).findFirst();

    return linkContentType.map(fLinkType -> vocabularies.contains(fLinkType)? VOCABULARY : NESTED).orElse(NESTED);
  }

}
