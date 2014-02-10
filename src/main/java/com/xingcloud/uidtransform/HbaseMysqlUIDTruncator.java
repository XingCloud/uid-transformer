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
      bytes = FileUtils.toBytes(hashedUIDs[i]);
      newBytes = new byte[bytes.length];
      System.arraycopy(bytes, 4, newBytes, 4, 4);
      resultUIDs[i] = FileUtils.toLong(newBytes);
    }
    return resultUIDs;
  }

  public static void truncate(String fileInputPath) throws Exception {
    final String uidKeywords = "uid";
    final char c = '\n';
    File f = new File(fileInputPath);
    File f2 = new File(fileInputPath + ".truncated");
    String line;
    long uid;
    byte[] bytes, newBytes;
    try (BufferedReader br = new BufferedReader(new FileReader(f)); PrintWriter pw = new PrintWriter(
      new FileWriter(f2));) {
      while ((line = br.readLine()) != null) {
        if (uidKeywords.equals(line)) {
          continue;
        }
        uid = Long.parseLong(line.trim());
        bytes = FileUtils.toBytes(uid);
        newBytes = new byte[bytes.length];
        System.arraycopy(bytes, 4, newBytes, 4, 4);
        uid = FileUtils.toLong(newBytes);
        pw.write(String.valueOf(uid));
        pw.write(c);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    truncate("D:/misc/fhw.lang.en_us.last_login.20140101.log");
//    int dateInt = 20140101;
//    String filePath = "D:/misc/fhw.first_pay_time.uid/fhw.first_pay_time.uid.";
//    for (int i = 0; i < 12; i++) {
//      truncate(filePath + (dateInt + i));
//    }
  }
}
