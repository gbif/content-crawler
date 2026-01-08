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
package org.gbif.content.crawl.contentful.backup;

import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.File;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to populate a contentful space from a backup.
 */
@Command(name = "contentful-restore", description = "Restore Contentful data from backup")
@MetaInfServices(Callable.class)
public class ContentfulRestoreCommand implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(ContentfulRestoreCommand.class);

  @Option(names = {"-c", "--config"}, description = "Configuration file path", required = true)
  private String configFile;

  /**
   * Executes the restore.
   */
  @Override
  public Integer call() {
    try {
      // Load configuration from file
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      ContentCrawlConfiguration configuration = mapper.readValue(new File(configFile), ContentCrawlConfiguration.class);
      
      LOG.info("Starting Contentful restore with config: {}", configFile);
      new ContentfulRestore(configuration).run();
      LOG.info("Contentful restore completed successfully");
      return 0;
    } catch (Exception e) {
      LOG.error("Restore failed!", e);
      return 1;
    }
  }
}
