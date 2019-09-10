package org.gbif.content.crawl.mendeley;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

/**
 * Writes each response as a file.
 */
public class ResponseToFileHandler implements ResponseHandler {

  private int pageNumber;

  private final File targetDir;

  public ResponseToFileHandler(File targetDir) {
    try {
      this.targetDir = Files.createDirectories(Paths.get(targetDir.getPath(),  Long.toString(new Date().getTime()))).toFile();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public void handleResponse(String responseAsJson) throws Exception {
    File targetFile = new File(targetDir, "page_" + pageNumber + ".response.json");
    try (Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFile),
                                                                StandardCharsets.UTF_8.name()))) {
      out.write(responseAsJson);
    }
    pageNumber += 1;
  }

  public File getTargetDir() {
    return targetDir;
  }

  @Override
  public void rollback() throws Exception {
    //NOP
  }

  @Override
  public void finish() {
    //NOP
  }
}
