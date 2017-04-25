package org.gbif.content.crawl.contentful.backup;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to trigger the backup of Contentful.
 */
@MetaInfServices(Command.class)
public class ContentfulBackupCommand extends BaseCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ContentfulBackupCommand.class);
  private final ContentCrawlConfiguration configuration = new ContentCrawlConfiguration();

  /**
   * Default constructor, sets the command name.
   */
  public ContentfulBackupCommand() {
    super("contentful-backup");
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
      new ContentfulBackup(configuration).run();
    } catch (IOException e) {
      LOG.error("Backup failed!", e);
      System.exit(1); // to enable triggering of e.g. an email from a Cron
    }
  }
}
