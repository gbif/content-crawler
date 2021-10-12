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
package org.gbif.content.crawl.mendeley;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;

/**
 * Iterables used to retrieved data from the GBIF Registry.
 */
public class RegistryIterables {

  /**
   * Private constructor.
   */
  private RegistryIterables(){
    //NOP
  }

  /**
   * Iterates thru all datasets associated to a DOI.
   */
  public static PageableIterable<Dataset> ofListByDoi(DatasetService service, String doi) {
    return PageableIterable.of( pagingRequest ->  service.listByDOI(doi, pagingRequest));
  }

  /**
   * Iterates thru all datasets usages of a download key.
   */
  public static PageableIterable<DatasetOccurrenceDownloadUsage> ofDatasetUsages(OccurrenceDownloadService service,
                                                                                 String downloadKey) {
    return PageableIterable.of( pagingRequest ->  service.listDatasetUsages(downloadKey, pagingRequest));
  }
}
