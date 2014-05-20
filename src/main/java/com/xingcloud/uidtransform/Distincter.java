package com.xingcloud.uidtransform;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * User: Z J Wu Date: 14-5-19 Time: 下午6:27 Package: com.xingcloud.uidtransform
 */
public class Distincter {
  private static final long ONE_MB = 1024 * 1024;

  public static void distinct(File input, File output) throws IOException {
    System.out.println("Distinct " + input.getAbsolutePath() + " into " + output.getAbsolutePath());
    Set<String> set = new HashSet<>(1000);
    LineIterator li = FileUtils.lineIterator(input);
    while (li.hasNext()) {
      set.add(li.next());
    }
    FileUtils.writeLines(output, set, true);
  }

  public static void main(String[] args) throws Exception {
    if (ArrayUtils.isEmpty(args) || args.length < 1) {
      throw new Exception("Parameter is not enough.");
    }
    String rawFile = args[0];
    long maxMemSize;
    if (args.length == 2) {
      maxMemSize = Long.valueOf(args[1]) * ONE_MB;
    } else {
      maxMemSize = ONE_MB;
    }
    File f = new File(rawFile);
    long fileLength = f.length();
    long i = fileLength / maxMemSize;
    int partNumber = (int) ((fileLength % maxMemSize == 0) ? i : (i + 1));
    File[] files = XAFileUtils.splitFile(f, partNumber);
    File out = new File(rawFile + ".distinct");
    for (File file : files) {
      distinct(file, out);
    }
    System.out.println("All done.");
  }
}
