package com.xingcloud.uidtransform;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 14-1-7 Time: 下午4:08 Package: com.xingcloud.uidtransform
 */
public enum StreamLogUidTransformer {
  INSTANCE;

  private BasicDataSource ds;

  private final int BATCH_SIZE = 50;

  private final char NEW_LINE = '\n';
  private final String EMPTY_LINE = "";

  private void close(AutoCloseable autoCloseable) {
    if (autoCloseable != null) {
      try {
        autoCloseable.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private StreamLogUidTransformer() {
    ds = new BasicDataSource();
    Collection<String> initSql = new ArrayList<>(1);
    initSql.add("select 1;");
    ds.setConnectionInitSqls(initSql);

    ds.setDriverClassName("com.mysql.jdbc.Driver");
    ds.setUsername("xingyun");
    ds.setPassword("xa");
    ds.setUrl("jdbc:mysql://65.255.35.134");
  }

  public String[] executeSqlFake(String projectId, long[] uids) {
    String[] strings = new String[uids.length];
    for (int i = 0; i < uids.length; i++) {
      strings[i] = String.valueOf(uids[i]);
    }
    return strings;
  }

  public String[] executeSqlTrue(String projectId, long[] uids) throws SQLException {
    if (ArrayUtils.isEmpty(uids)) {
      return null;
    }
    String[] strings = new String[uids.length];
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    StringBuilder sql = new StringBuilder("select t.id, t.orig_id from `vf_");
    sql.append(projectId);
    sql.append("`.id_map as t where t.id in (");
    sql.append(uids[0]);
    char comma = ',';
    for (int i = 1; i < uids.length; i++) {
      sql.append(comma);
      sql.append(uids[i]);
    }
    sql.append(')');
    Map<Long, String> idmap = new HashMap<>(uids.length);
    try {
      conn = ds.getConnection();
      pstmt = conn.prepareStatement(sql.toString());
//      System.out.println(sql.toString());
      rs = pstmt.executeQuery();
      while (rs.next()) {
        idmap.put(rs.getLong(1), rs.getString(2));
      }
    } finally {
      close(conn);
      close(pstmt);
      close(rs);
    }
    for (int i = 0; i < uids.length; i++) {
      strings[i] = idmap.get(uids[i]);
    }
    return strings;
  }

  public String[] executeSql(String projectId, long[] uids, boolean debug) throws SQLException {
    return debug ? executeSqlFake(projectId, uids) : executeSqlTrue(projectId, uids);
  }

  public void writeString(String[] lines, PrintWriter pw) {
    if (ArrayUtils.isEmpty(lines)) {
      return;
    }
    for (String line : lines) {
      if (StringUtils.isBlank(line)) {
        pw.write(EMPTY_LINE);
      } else {
        pw.write(line);
      }
      pw.write(NEW_LINE);
    }
  }

  public List<String> transform(String projectId, List<Long> internalUIDs, boolean debug) throws IOException,
    SQLException {
    int size = internalUIDs.size();
    long[] internalUIDArray = new long[size];
    for (int i = 0; i < size; i++) {
      internalUIDArray[i] = internalUIDs.get(i);
    }
    String[] origUidArr = executeSql(projectId, internalUIDArray, debug);
    return Arrays.asList(origUidArr);
  }

  public void transform(String projectId, String inputInternalUIDFilePath, boolean debug) throws IOException,
    SQLException {
    File inputFile = new File(inputInternalUIDFilePath);
    File oriPath = new File(inputFile.getParent() + "/original");
    if (!oriPath.exists()) {
      oriPath.mkdir();
    }
    File output = new File(oriPath.getAbsolutePath() + "/" + inputFile.getName());
    String line;
    long[] internalUIDs = new long[BATCH_SIZE];
    int counter = 0;
    String[] originalUIDs;
    int fileLines = FileUtils.count(inputInternalUIDFilePath), sum = 0;
    String percent, lastPercent = null;
    try (BufferedReader br = new BufferedReader(new FileReader(inputFile)); PrintWriter pw = new PrintWriter(
      new FileWriter(output))) {

      while ((line = br.readLine()) != null) {
        if (StringUtils.isBlank(line) || "uid".equals(line)) {
          continue;
        }
        if (counter == BATCH_SIZE) {
          originalUIDs = executeSql(projectId, internalUIDs, debug);
          writeString(originalUIDs, pw);
          counter = 0;
          sum += BATCH_SIZE;
          percent = FileUtils.percent(sum, fileLines);
          if (!percent.equals(lastPercent)) {
            System.out.println("[" + percent + "]");
            lastPercent = percent;
          }
        }
        internalUIDs[counter] = Long.valueOf(line.trim());
        ++counter;
      }
      long[] lastInternalUIDs = new long[counter];
      System.arraycopy(internalUIDs, 0, lastInternalUIDs, 0, counter);
      originalUIDs = executeSql(projectId, lastInternalUIDs, debug);
      writeString(originalUIDs, pw);

      sum += counter;
      percent = FileUtils.percent(sum, fileLines);
      if (!percent.equals(lastPercent)) {
        System.out.println("[" + percent + "]");
      }
    }
  }

  public static void main(String[] args) throws IOException, SQLException {
    if (args == null || args.length < 1) {
      System.exit(1);
    }
    File uidFileRoot = new File(args[0]);
    String projectId, fileName;

    if (uidFileRoot.isFile()) {
      fileName = uidFileRoot.getName();
      projectId = fileName.substring(0, fileName.indexOf('.'));
      StreamLogUidTransformer.INSTANCE.transform(projectId, uidFileRoot.getAbsolutePath(), false);
    } else {
      File[] files = uidFileRoot.listFiles();
      for (File file : files) {
        if (file.isDirectory()) {
          continue;
        }
        fileName = file.getName();
        System.out.println(fileName);
        projectId = fileName.substring(0, fileName.indexOf('.'));
        StreamLogUidTransformer.INSTANCE.transform(projectId, file.getAbsolutePath(), false);
      }
    }
  }
}
