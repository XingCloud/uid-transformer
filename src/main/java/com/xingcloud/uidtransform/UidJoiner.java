package com.xingcloud.uidtransform;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * User: Z J Wu Date: 14-5-16 Time: 下午1:46 Package: com.xingcloud.uidtransform
 */
public class UidJoiner {
  private static final DecimalFormat df = new DecimalFormat("00000");
  private static final String PART_KEYWORD = "part";

  private static String getSuffix(int i) {
    return "." + PART_KEYWORD + df.format(i);
  }

  private static void joinUID(File left, File right, File out) throws IOException {
    Set<String> leftSet = new HashSet<>(1000);
    Set<String> rightSet = new HashSet<>(1000);
    LineIterator li = FileUtils.lineIterator(left);
    while (li.hasNext()) {
      leftSet.add(li.next());
    }
    li = FileUtils.lineIterator(right);
    while (li.hasNext()) {
      rightSet.add(li.next());
    }
    FileUtils.writeLines(out, CollectionUtils.intersection(leftSet, rightSet), true);
  }

  public static File[] splitFile(File file, int part) throws IOException {
    File[] partFiles = new File[part];
    PrintWriter[] writers = new PrintWriter[part];
    String parent = file.getParent();
    String name = file.getName();
    String tmp = parent + File.separatorChar + "tmp";
    File tmpFile = new File(tmp);
    if (!tmpFile.exists()) {
      tmpFile.mkdir();
    }
    for (int i = 0; i < part; i++) {
      partFiles[i] = new File(tmp + File.separatorChar + name + getSuffix(i));
      writers[i] = new PrintWriter(new OutputStreamWriter(new FileOutputStream(partFiles[i])));
    }
    String line;
    int index;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      while ((line = br.readLine()) != null) {
        line = StringUtils.trimToNull(line);
        if (StringUtils.isBlank(line)) {
          continue;
        }
        index = line.hashCode() % part;
        writers[index].write(line);
        writers[index].write('\n');
      }
    } finally {
      for (PrintWriter pw : writers) {
        IOUtils.closeQuietly(pw);
      }
    }
    return partFiles;
  }

  public static void main(String[] args) throws Exception {
    if (ArrayUtils.isEmpty(args) || args.length < 4) {
      throw new Exception("Parameter is not enough.");
    }
    long OneMB = 1024 * 1024;
    String left, right, outPath;
    left = args[0];
    right = args[1];
    outPath = args[2];
    long batchSize = OneMB * Long.valueOf(args[3]);

    File leftFile = new File(left);
    long leftFileLength = leftFile.length();
    File rightFile = new File(right);
    long rightFileLength = rightFile.length();
    long bigOne = Math.max(leftFileLength, rightFileLength);
    long i = bigOne / batchSize;
    int partNumber = (int) ((bigOne % batchSize == 0) ? i : (i + 1));
    File[] leftFiles = splitFile(leftFile, partNumber);
    File[] rightFiles = splitFile(rightFile, partNumber);
    String outFileName = leftFile.getName() + ".join." + rightFile.getName();
    String outFilePath = outPath + File.separatorChar + outFileName;
    File out = new File(outFilePath);
    if (out.exists()) {
      out.delete();
    }
    for (int j = 0; j < i; j++) {
      joinUID(leftFiles[j], rightFiles[j], out);
    }
  }
}
