package com.xingcloud.uidtransform;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;

/**
 * User: Z J Wu Date: 14-1-8 Time: 上午9:55 Package: com.xingcloud.uidtransform
 */
public class XAFileUtils {

  private static final DecimalFormat DF_PART = new DecimalFormat("00000");
  private static final String PART_KEYWORD = "part";

  public static final int FILE_READ_BUFFERED_SIZE = 4096;

  public static int count(String filename) throws IOException {
    byte[] c = new byte[FILE_READ_BUFFERED_SIZE];
    int count = 0, readChars;
    boolean empty = true;
    try (InputStream is = new BufferedInputStream(new FileInputStream(filename));) {
      while ((readChars = is.read(c)) != -1) {
        empty = false;
        for (int i = 0; i < readChars; ++i) {
          if (c[i] == '\n') {
            ++count;
          }
        }
      }
      return (count == 0 && !empty) ? 1 : count;
    }
  }

  public static String percent(int sum, int total) {
    double d = sum * 100.0 / total;
    int p = (int) Math.floor(d);
    char[] chars = new char[100];
    for (int i = 0; i < p; i++) {
      chars[i] = '#';
    }
    for (int i = p; i < 100; i++) {
      chars[i] = '.';
    }
    return new String(chars);
//    return DF.format(d);
  }

  private static int SIZEOF_LONG = 8;

  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  public static long toLong(byte[] bytes) throws Exception {
    return toLong(bytes, 0, SIZEOF_LONG);
  }

  public static long toLong(byte[] bytes, int offset, final int length) throws Exception {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw new Exception(
        "to long exception " + "offset " + offset + " length " + length + " bytes.len " + bytes.length);
    }
    long l = 0;
    for (int i = offset; i < offset + length; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xFF;
    }
    return l;
  }

  private static String getSuffix(int i) {
    return "." + PART_KEYWORD + DF_PART.format(i);
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
      System.out.println("Part file of " + file.getName() + " - " + partFiles[i].getAbsolutePath());
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
        index = Math.abs(line.hashCode() % part);
        try {
          writers[index].write(line);
          writers[index].write('\n');
        } catch (Exception e) {
          System.out.println("Error index: " + index);
        }
      }
    } finally {
      for (PrintWriter pw : writers) {
        IOUtils.closeQuietly(pw);
      }
    }
    return partFiles;
  }

  public static void main(String[] args) throws IOException {
    System.out.println(percent(534, 534) + "%");
  }
}
