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

import java.util.Map;
import java.util.Optional;

import com.contentful.java.cda.CDAEntry;

/**
 * This class represents the metadata created when a resource was imported from Drupal.
 */
public class Meta {

  //Known field names
  private static final String META_FIELD = "meta";
  private static final String DRUPAL_FIELD = "drupal";
  private static final String CREATED_FIELD = "created";

  /**
   * Private constructor.
   */
  private Meta() {
    //NOP
  }

  /**
   * Extracts the Meta information from the CDAEntry.
   */
  public static Optional<String> getMetaCreatedDate(CDAEntry cdaEntry) {
    return Optional.ofNullable(cdaEntry.getField(META_FIELD))
              .map(metaValue ->
                (String)((Map<String,Object>)((Map<String,Object>)metaValue).get(DRUPAL_FIELD)).get(CREATED_FIELD)
              );
  }

}
