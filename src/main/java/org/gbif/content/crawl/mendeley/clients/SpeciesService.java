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
package org.gbif.content.crawl.mendeley.clients;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping(value = "/species", produces = MediaType.APPLICATION_JSON_VALUE)
public interface SpeciesService {

  @GetMapping("{speciesKey}")
  NameUsage get(@PathVariable("speciesKey") Integer key);

  static SpeciesService wsClient(String gbifApiUrl) {
    ObjectMapper objectMapper = JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    
    return new ClientBuilder()
            .withUrl(gbifApiUrl)
            .withObjectMapper(objectMapper)
            .build(SpeciesService.class);
  }
}
