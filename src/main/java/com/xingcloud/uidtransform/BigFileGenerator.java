package com.xingcloud.uidtransform;

import java.io.*;
import java.util.Random;

/**
 * User: Z J Wu Date: 14-5-16 Time: 下午2:02 Package: com.xingcloud.uidtransform
 */
public class BigFileGenerator {
  public static void main(String[] args) throws FileNotFoundException {
    File f = new File("d:/misc/smallFile2.txt");
    Random random = new Random();
    try {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f)));
      for (int i = 0; i < 1000000; i++) {
        pw.write(String.valueOf(random.nextInt(100000)));
        pw.write('\n');
      }
    } catch (IOException e) {
        e.printStackTrace();
    }
  }
}
