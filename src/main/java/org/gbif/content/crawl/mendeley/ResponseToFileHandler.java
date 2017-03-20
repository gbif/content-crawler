package org.gbif.content.crawl.mendeley;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;


/**
 * Writes each response as a file.
 */
public class ResponseToFileHandler implements ResponseHandler {

  private int pageNumber;

  private final File targetDir;

  public ResponseToFileHandler(File targetDir) {
    this.targetDir = targetDir;
    targetDir.mkdirs();
  }

  @Override
  public void handleResponse(String responseAsJson) throws Exception {
    File targetFile = new File(targetDir, "page_" + pageNumber + ".response.json");
    try (
      Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFile), "UTF-8"));
    ) {
      out.write(responseAsJson);
    }
    pageNumber +=1;
  }

  @Override
  public void finish() {
    //NOP
  }
}
