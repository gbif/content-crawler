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
