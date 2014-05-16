package com.xingcloud.uidtransform;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

/**
 * User: Z J Wu Date: 14-1-13 Time: 上午10:22 Package: com.xingcloud.uidtransform
 */
public class HbaseMysqlUIDTruncator {

  public static List<Long> truncate(List<String> uidStr) throws Exception {
    int size = uidStr.size();
    long[] longs = new long[size];
    for (int i = 0; i < size; i++) {
      longs[i] = Long.valueOf(uidStr.get(i));
    }
    return Arrays.asList(truncate(longs));
  }

  public static Long[] truncate(long... hashedUIDs) throws Exception {
    byte[] bytes, newBytes;
    Long[] resultUIDs = new Long[hashedUIDs.length];
    for (int i = 0; i < hashedUIDs.length; i++) {
      bytes = XAFileUtils.toBytes(hashedUIDs[i]);
      newBytes = new byte[bytes.length];
      System.arraycopy(bytes, 4, newBytes, 4, 4);
      resultUIDs[i] = XAFileUtils.toLong(newBytes);
    }
    return resultUIDs;
  }

  public static void truncate(String fileInputPath) throws Exception {
    final String uidKeywords = "uid";
    final char c = '\n';
    File f = new File(fileInputPath);
    File f2 = new File(f.getParentFile() + "/truncated");
    if (!f2.exists()) {
      f2.mkdir();
    }
    File f3 = new File(f2.getAbsolutePath() + "/" + f.getName());
    String line;
    long uid;
    byte[] bytes, newBytes;
    try (BufferedReader br = new BufferedReader(new FileReader(f)); PrintWriter pw = new PrintWriter(
      new FileWriter(f3));) {
      while ((line = br.readLine()) != null) {
        if (uidKeywords.equals(line)) {
          continue;
        }
        uid = Long.parseLong(line.trim());
        bytes = XAFileUtils.toBytes(uid);
        newBytes = new byte[bytes.length];
        System.arraycopy(bytes, 4, newBytes, 4, 4);
        uid = XAFileUtils.toLong(newBytes);
        pw.write(String.valueOf(uid));
        pw.write(c);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 1) {
      System.out.println("No param.");
      System.exit(1);
    }
    String uidRoot = args[0];
    File uidRootFile = new File(uidRoot);
    if (uidRootFile.isFile()) {
      truncate(uidRootFile.getAbsolutePath());
    } else {
      File[] files = uidRootFile.listFiles();
      for (File f : files) {
        if (f.isDirectory()) {
          continue;
        }
        truncate(f.getAbsolutePath());
      }
    }
  }
}
