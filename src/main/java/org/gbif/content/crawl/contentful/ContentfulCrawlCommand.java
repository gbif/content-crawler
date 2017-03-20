package org.gbif.content.crawl.contentful;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contentful crawl command. Triggers  crawl using the specified configuration.
 */
@MetaInfServices(Command.class)
public class ContentfulCrawlCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(ContentfulCrawlCommand.class);

  private  final ContentCrawlConfiguration configuration = new ContentCrawlConfiguration();

  /**
   * Default constructor, sets the command name.
   */
  public ContentfulCrawlCommand() {
    super("contentful-crawl");
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
    new ContentfulCrawler(configuration).run();
  }
}
