package org.gbif.content.crawl;

import org.gbif.content.crawl.contentful.backup.ContentfulBackupCommand;
import org.gbif.content.crawl.contentful.backup.ContentfulRestoreCommand;
import org.gbif.content.crawl.contentful.crawl.ContentfulCrawlCommand;
import org.gbif.content.crawl.mendeley.MendeleyCrawlCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Main CLI application that coordinates all content crawler commands.
 */
@Command(
    name = "content-crawler",
    description = "GBIF Content Crawler - crawl and index content to Elasticsearch",
    subcommands = {
        ContentfulBackupCommand.class,
        ContentfulRestoreCommand.class,
        ContentfulCrawlCommand.class,
        MendeleyCrawlCommand.class,
        CommandLine.HelpCommand.class
    }
)
public class ContentCrawlerMain {
    
    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new ContentCrawlerMain());
        
        if (args.length == 0) {
            commandLine.usage(System.out);
            System.exit(1);
        }
        
        int exitCode = commandLine.execute(args);
        System.exit(exitCode);
    }
} 