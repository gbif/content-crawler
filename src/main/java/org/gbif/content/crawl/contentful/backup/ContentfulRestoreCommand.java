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

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to populate a contentful space from a backup.
 */
@MetaInfServices(Command.class)
public class ContentfulRestoreCommand extends BaseCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ContentfulRestoreCommand.class);
  private final ContentCrawlConfiguration configuration = new ContentCrawlConfiguration();

  /**
   * Default constructor, sets the command name.
   */
  public ContentfulRestoreCommand() {
    super("contentful-restore");
  }

  /**
   * Configuration object.
   */
  @Override
  protected ContentCrawlConfiguration getConfigurationObject() {
    return configuration;
  }

  /**
   * Executes and asynchronous crawl.
   */
  @Override
  protected void doRun() {
    try {
      new ContentfulRestore(configuration).run();
    } catch (Exception e) {
      LOG.error("Restore failed!", e);
      System.exit(1); // to enable triggering of e.g. an email from a Cron
    }
  }
}
