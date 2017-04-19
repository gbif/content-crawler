package org.gbif.content.crawl.contentful;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;

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
    } catch (IOException e) {
      LOG.error("Restore failed!", e);
      System.exit(1); // to enable triggering of e.g. an email from a Cron
    }
  }
}
