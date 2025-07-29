/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.content.crawl.contentful.crawl;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.contentful.java.cma.Constants.CMAFieldType;
import com.contentful.java.cma.model.CMAContentType;
import com.contentful.java.cma.model.CMAField;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Generates the ElasticSearch mapping of a Contentful ContentType.
 */
public class  MappingGenerator {

  private static final String KEYWORD = "keyword";
  private static final String TEXT = "text";
  private static final String NESTED = "nested";
  private static final String VOCABULARY = "vocabulary";
  private static final String TITLE_FIELD = "title";

  private static final Pattern ES_COMPLEX_TYPES = Pattern.compile("nested|object");

  /**
   * Fields that are stored but not indexed/analised.
   */
  private static final Pattern IGNORED_FIELDS = Pattern.compile("space|revision|type");

  private static final Pattern FULLTEXT_FIELDS = Pattern.compile("title|body|description|summary");

  /**
   * List of types that can be obtained using a non-localized version.
   */
  public static final Set<CMAFieldType> COLLAPSIBLE_TYPES = EnumSet.of(CMAFieldType.Boolean);

  /**
   * List of FIELDS that can be obtained using a non-localized version.
   */
  public static final Pattern COLLAPSIBLE_FIELDS = Pattern.compile("meta");

  /**
   * Fields that are common to all contentful content types.
   */
  private static final Map<String, String> KNOWN_FIELDS = new ImmutableMap.Builder<String, String>()
    .put("search_text",TEXT)
    .put("locale", KEYWORD )
    .put("contentType", KEYWORD)
    .put("id", KEYWORD)
    .put("createdAt", "date")
    .put("updatedAt", "date")
    .put("revision", "float")
    .put("gbifRegion", KEYWORD)
    .put("gbifProgrammeAcronym", KEYWORD)
    .put("type", KEYWORD).build();

  /**
   * Mapping of Contentful to ElasticSearch data types.
   */
  private static final Map<CMAFieldType,String> CONTENTFUL_ES_TYPE_MAP = new ImmutableMap.Builder<CMAFieldType, String>()
    .put(CMAFieldType.Symbol, KEYWORD)
    .put(CMAFieldType.Text, TEXT)
    .put(CMAFieldType.Boolean, "boolean")
    .put(CMAFieldType.Date, "date")
    .put(CMAFieldType.Object, NESTED)
    .put(CMAFieldType.Location, "geo_point")
    .put(CMAFieldType.Integer, "integer")
    .put(CMAFieldType.Number, "double")
    .build();

  //Commonly used constants
  private static final String TYPE = "type";
  private static final String LINK_CONTENT_TYPE = "linkContentType";
  private static final String VALIDATIONS = "validations";

  /**
   * List of content types that store vocabularies.
   */
  private final Collection<String> vocabularies;

  /**
   * All KNOWN_FIELDS are mapped to data type 'keyword'.
   */
  private static void addProperties(ObjectNode mapping, Map<String, String> collapsedFields) {
    Map<String,String> flatFields = new HashMap<>(collapsedFields);
    flatFields.putAll(KNOWN_FIELDS);
    ObjectNode properties = mapping.putObject("properties");
    flatFields.forEach((key,type) -> {
        try {
          ObjectNode fieldNode = properties.putObject(key);
            fieldNode.put("type", type);
            if (KEYWORD.equals(type) || TEXT.equals(type)) {
              fieldNode.put("copy_to", "search_text");
            }
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
    });
  }

  /**
   * Ignored fields mapping.
   * Produces the following JSON structure:
   * {
   *  "ignored_fields": {
   *    "match": "space|revision|type",
   *    "match_pattern": "regex",
   *    "mapping": {
   *      "index": false
   *    }
   *  }
   * }
   */
  private static void addDefaultIgnoredFields(ArrayNode dynamicTemplate) {
    // Simple ignored fields
    ObjectNode template = dynamicTemplate.addObject();
    ObjectNode ignoredFields = template.putObject("ignored_fields");
    ignoredFields.put("match", IGNORED_FIELDS.pattern());
    ignoredFields.put("match_pattern", "regex");
    ObjectNode ignoredMapping = ignoredFields.putObject("mapping");
    ignoredMapping.put("enabled", false);
  }

  /**
   * This is used to create links between content types, in particular in the News content type.
   */
  private static void addGenericTagsMapping(ArrayNode dynamicTemplate) {
    ObjectNode template = dynamicTemplate.addObject();
    ObjectNode genericTags = template.putObject("generic_tags");
      genericTags.put("match", ".*Tag");
      genericTags.put("match_pattern", "regex");
      ObjectNode genericTagsMapping = genericTags.putObject("mapping");
        genericTagsMapping.put("type", KEYWORD);
        genericTagsMapping.put("copy_to", "search_text");
  }

  /**
   * Adds the file asset mapping.
   * {
   *   "asset_files": {
   *     "path_match": "*.file.*",
   *     "mapping": {
   *       "index": false
   *     }
   *   }
   * }
   */
  private static void addFileAssetMapping(ArrayNode dynamicTemplate) {
    ObjectNode template = dynamicTemplate.addObject();
    ObjectNode assetFiles = template.putObject("asset_files");
      assetFiles.put("path_match", "*.file.*");
      ObjectNode assetFilesMapping = assetFiles.putObject("mapping");
        assetFilesMapping.put("type", "object");
        assetFilesMapping.put("enabled", false);
  }

  /**
   * Adds nested mapping for a field.
   * {
   *   "nested_fieldName": {
   *     "path_match": "*.fieldName.*",
   *     "mapping": {
   *       "type": "esType"
   *     }
   *   }
   * }
   */
  private static void addNestedMapping(ArrayNode dynamicTemplate, String fieldName, String esType) {
    ObjectNode template = dynamicTemplate.addObject();
    ObjectNode nestedField = template.putObject("nested_" + fieldName);
      nestedField.put("path_match",  "*." + fieldName + ".*");
      ObjectNode nestedMapping = nestedField.putObject("mapping");
        nestedMapping.put("type", esType);
        if (KEYWORD.equals(esType) || TEXT.equals(esType)) {
          nestedMapping.put("copy_to", "search_text");
        }
  }

  /**
   * Adds match mapping for a field.
   * {
   *   "nested_fieldName": {
   *     "match": "fieldName",
   *     "mapping": {
   *       "type": "esType"
   *     }
   *   }
   * }
   */
    private static void addMatchMapping(ArrayNode dynamicTemplate, String fieldName, String esType) {
      ObjectNode template = dynamicTemplate.addObject();
      ObjectNode nestedField = template.putObject("nested_" + fieldName);
         nestedField.put("match",  fieldName);
         ObjectNode nestedMapping = nestedField.putObject("mapping");
           nestedMapping.put("type", esType);
    }
  /**
   * Adds a template field to the mapping.
   * {
   *   "fieldName": {
   *     "match": "fieldPattern",
   *     "mapping": {
   *       "type": "esType"
   *     }
   *   }
   * }
   */
  public static void addTemplateField(String match, String fieldName, String fieldPattern, String esType,
                                       ArrayNode dynamicMapping) {
    try {
      ObjectNode template = dynamicMapping.addObject();
      ObjectNode fieldNode = template.putObject(fieldName);
        fieldNode.put(match, fieldPattern);
        ObjectNode fieldMapping = fieldNode.putObject("mapping");
          fieldMapping.put("type", esType);
          if (NESTED.equals(esType)) {
              fieldMapping.put("dynamic", true);
          } else if (FULLTEXT_FIELDS.matcher(fieldName).matches()) {
            fieldMapping.put("copy_to", "search_text");
          }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Checks if the CDAField represents a Link to another resource.
   */
  private static boolean isLink(CMAField cmaField) {
    return cmaField.getType() == CMAFieldType.Link
           || (cmaField.getType() == CMAFieldType.Array
               && cmaField.getArrayItems().get(TYPE).equals(CMAFieldType.Link.name()));
  }

  /**
   * Validates if the field and the derived elasticsearch type define a simple type, i.e.: non-nested elements.
   */
  private static boolean isSimpleField(CMAField cmaField, String esType) {
      return COLLAPSIBLE_TYPES.contains(cmaField.getType())
              || COLLAPSIBLE_FIELDS.matcher(cmaField.getId()).matches()
              || (!ES_COMPLEX_TYPES.matcher(esType).matches() && !cmaField.isLocalized());
  }

  /**
   * Default constructor.
   */
  public MappingGenerator(Collection<CMAContentType> vocabularies) {
    this.vocabularies = vocabularies.stream().map(CMAContentType::getId).collect(Collectors.toSet());
  }

  /**
   * Generates the mappings file for ElasticSearch index.
   * In general terms it will produce a structure like:
   * {
   *   "dynamic_templates": [
   *   {
   *      "ignored_fields": {
   *        "match": "images|space|revision|type",
   *        "match_pattern": "regex",
   *        "mapping": {
   *            "index": false
   *          }
   *        }
   *      }
   *      {
   *        "template1" : {
   *          "match|path_match": fieldPattern, (in general match or path_match are used)
   *          "mapping": {
   *            "type": dataType
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
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode mapping = mapper.createObjectNode();
    ArrayNode dynamicTemplates = mapping.putArray("dynamic_templates");
    addDefaultIgnoredFields(dynamicTemplates);
    addFileAssetMapping(dynamicTemplates);
    addGenericTagsMapping(dynamicTemplates);
    addNestedMapping(dynamicTemplates, TITLE_FIELD, TEXT);
    addNestedMapping(dynamicTemplates, "description", TEXT);
    addNestedMapping(dynamicTemplates, "summary", TEXT);
    addNestedMapping(dynamicTemplates, "body", TEXT);
    addNestedMapping(dynamicTemplates, "title", TEXT);
    addMatchMapping(dynamicTemplates, "id", KEYWORD);
    addMatchMapping(dynamicTemplates, "isoCode", KEYWORD);
    addNestedMapping(dynamicTemplates, "label", KEYWORD);
    addNestedMapping(dynamicTemplates, "url", KEYWORD);
    contentType.getFields().stream()
      .filter(cmaField -> !cmaField.isDisabled()).forEach(cmaField ->
      esType(cmaField).ifPresent(esType -> {
        if (VOCABULARY.equals(esType)) {
          collapsedFields.put(cmaField.getId(), KEYWORD);
        } else if (isSimpleField(cmaField, esType)) {
          collapsedFields.put(cmaField.getId(), esType);
        } else if (!NESTED.equals(esType) && cmaField.isLocalized() && !cmaField.getName().equalsIgnoreCase("blocks")) { //localizable fields have nested elements
          addTemplateField("path_match", cmaField.getId(), cmaField.getId() + ".*", esType, dynamicTemplates);
        } else if (!cmaField.getName().equalsIgnoreCase("blocks")) {
          addTemplateField("match", cmaField.getId(), cmaField.getId(), esType, dynamicTemplates);
        }
      })
    );
    addProperties(mapping, collapsedFields);
    return mapping.toPrettyString();
  }

  /**
   * Gets the ElasticSearch deduced type of the CDAField.
   */
  private Optional<String> esType(CMAField cmaField) {
    if (isLink(cmaField)) {
      return Optional.ofNullable(getEsLinkType(cmaField));
    }
    if (cmaField.getType() == CMAFieldType.Array) {
      return Optional.ofNullable(CONTENTFUL_ES_TYPE_MAP.get(cmaField.getArrayItems().get(TYPE)));
    }
    if (TITLE_FIELD.equalsIgnoreCase(cmaField.getName())) {
      return Optional.of(TEXT);
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
        .map(validation ->
                //if the validation context is not a link return an empty response
                Optional.ofNullable((List<String>)validation.get(LINK_CONTENT_TYPE))
                        .map(validations -> validations.get(0))).findFirst().orElse(Optional.empty());

    return linkContentType.map(fLinkType -> vocabularies.contains(fLinkType)? VOCABULARY : NESTED).orElse(NESTED);
  }

}
