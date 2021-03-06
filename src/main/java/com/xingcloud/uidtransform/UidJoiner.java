package com.xingcloud.uidtransform;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * User: Z J Wu Date: 14-5-16 Time: 下午1:46 Package: com.xingcloud.uidtransform
 */
public class UidJoiner {
  public static enum JoinType {
    INNER, ANTI
  }

  private static void joinUID(JoinType joinType, File left, File right, File out) throws IOException {
    System.out.println(left.getAbsolutePath() + " [" + joinType + " JOIN] " + right.getAbsolutePath());
    Set<String> leftSet = new HashSet<String>(1000);
    Set<String> rightSet = new HashSet<String>(1000);
    LineIterator li = FileUtils.lineIterator(left);
    while (li.hasNext()) {
      leftSet.add(li.next());
    }
    li = FileUtils.lineIterator(right);
    while (li.hasNext()) {
      rightSet.add(li.next());
    }
    switch (joinType) {
      case INNER:
        FileUtils.writeLines(out, CollectionUtils.intersection(leftSet, rightSet), true);
        break;
      case ANTI:
        leftSet.removeAll(rightSet);
        FileUtils.writeLines(out, leftSet, true);
        break;
      default:
        FileUtils.writeLines(out, CollectionUtils.intersection(leftSet, rightSet), true);
    }
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
    JoinType joinType = JoinType.INNER;
    if (args.length == 5) {
      joinType = Enum.valueOf(JoinType.class, args[4]);
    }

    System.out.println("[LEFT-FILE]=" + left);
    System.out.println("[RIGHT-FILE]=" + right);
    System.out.println("[OUT-FILE]=" + outPath);
    System.out.println("[JOIN-TYPE]=" + joinType);
    System.out.println("[MAX-MEM]=" + (batchSize / 1024 / 1024) + " mb");

    File leftFile = new File(left);
    long leftFileLength = leftFile.length();
    File rightFile = new File(right);
    long rightFileLength = rightFile.length();
    long bigOne = Math.max(leftFileLength, rightFileLength);
    long i = bigOne / batchSize;
    int partNumber = (int) ((bigOne % batchSize == 0) ? i : (i + 1));
    System.out.println("[PARTS]=" + partNumber);

    File[] leftFiles = XAFileUtils.splitFile(leftFile, partNumber);
    File[] rightFiles = XAFileUtils.splitFile(rightFile, partNumber);
    String outFileName = leftFile.getName() + "." + joinType + "_JOIN." + rightFile.getName();
    String outFilePath = outPath + File.separatorChar + outFileName;
    File out = new File(outFilePath);
    if (out.exists()) {
      out.delete();
    }
    for (int j = 0; j < i; j++) {
      joinUID(joinType, leftFiles[j], rightFiles[j], out);
    }
    System.out.println("All done");
  }
}
