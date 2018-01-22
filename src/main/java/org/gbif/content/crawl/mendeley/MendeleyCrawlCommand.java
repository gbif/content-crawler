package org.gbif.content.crawl.mendeley;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.content.crawl.conf.ContentCrawlConfiguration;

import java.io.IOException;
import java.net.UnknownHostException;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CLI options for initiating Mendeley crawls.
 */
@MetaInfServices(Command.class)
public class MendeleyCrawlCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(MendeleyCrawlCommand.class);

  private final ContentCrawlConfiguration configuration = new ContentCrawlConfiguration();

  public MendeleyCrawlCommand() {
    super("mendeley-crawl");
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

  @Override
  protected void doRun() {
    try {
      MendeleyDocumentCrawler crawler = new MendeleyDocumentCrawler(configuration);
      crawler.run();
    } catch (UnknownHostException e) {
      LOG.error("Invalid configuration", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Error contacting remote endpoints", e);
      throw new RuntimeException(e);
    }
  }
}
