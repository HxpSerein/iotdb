/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.wal.buffer;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class WALConsistentTest {
  protected static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected static final boolean preIsClusterMode = config.isClusterMode();

  protected static final String logDirectory_72 = "D:/dev/data/72";
  protected static final String logDirectory_74 = "D:/dev/data/74";
  protected static final String logDirectory_75 = "D:/dev/data/75";

  protected static final String logDirectory_72_353 = "D:/dev/data/72/353";
  protected static final String logDirectory_74_353 = "D:/dev/data/74/353";
  protected static final String logDirectory_75_353 = "D:/dev/data/75/353";

  protected IWALBuffer walBuffer_72;
  protected IWALBuffer walBuffer_74;
  protected IWALBuffer walBuffer_75;

  @Before
  public void setUp() throws Exception {
    walBuffer_72 = new WALBuffer("72", logDirectory_72_353);
    walBuffer_74 = new WALBuffer("74", logDirectory_74_353);
    walBuffer_75 = new WALBuffer("75", logDirectory_75_353);
    config.setClusterMode(true);
  }

  @After
  public void tearDown() throws Exception {
    walBuffer_72.close();
    walBuffer_74.close();
    walBuffer_75.close();
    config.setClusterMode(preIsClusterMode);
  }

  static class Pair {
    long[] times;
    long searchIndex;

    Pair(long[] times, long searchIndex) {
      this.times = times;
      this.searchIndex = searchIndex;
    }
  }

  @Test
  public void readWALFile() throws Exception {
    File[] walFiles_72_353 = WALFileUtils.listAllWALFiles(new File(logDirectory_72_353));
    int rowCount_72 = getRowCount(walFiles_72_353);
    System.out.println(rowCount_72);
    //    Set<InsertTabletNode> actualInsertTabletNodes_73_167 =
    // getInsertTabletNodes(walFiles_73_167);
    //    System.out.println(actualInsertTabletNodes_73_167.size());

    File[] walFiles_74_353 = WALFileUtils.listAllWALFiles(new File(logDirectory_74_353));
    int rowCount_74 = getRowCount(walFiles_74_353);
    System.out.println(rowCount_74);
    //    Set<InsertTabletNode> actualInsertTabletNodes_74_167 =
    // getInsertTabletNodes(walFiles_74_167);
    //    System.out.println(actualInsertTabletNodes_74_167.size());

    File[] walFiles_75_353 = WALFileUtils.listAllWALFiles(new File(logDirectory_75_353));
    int rowCount_75 = getRowCount(walFiles_75_353);
    System.out.println(rowCount_75);
    //    Set<InsertTabletNode> actualInsertTabletNodes_75_167 =
    // getInsertTabletNodes(walFiles_75_167);
    //    System.out.println(actualInsertTabletNodes_75_167.size());
  }

  private static int getRowCount(File[] walFiles) throws IOException {
    int rowCount = 0;
    Arrays.sort(walFiles, new FileComparator());
    for (File walFile : walFiles) {
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          InsertTabletNode insertTabletNode = (InsertTabletNode) walReader.next().getValue();
          String path = insertTabletNode.getDevicePath().getNodes()[3];
          if (path.equals("d_530")) {
            rowCount += insertTabletNode.getRowCount();
          }
        }
      }
    }
    return rowCount;
  }

  @Test
  public void cmpSearchIndex() throws Exception {
    File[] walFiles_72_353 = WALFileUtils.listAllWALFiles(new File(logDirectory_72_353));
    Set<Pair> searchIndex_72 = getSearchIndex(walFiles_72_353);

    File[] walFiles_74_353 = WALFileUtils.listAllWALFiles(new File(logDirectory_74_353));
    Set<Pair> searchIndex_74 = getSearchIndex(walFiles_74_353);

    long disappearedIndex = 0;

    for (Pair pair_72 : searchIndex_72) {
      long[] times_72 = pair_72.times;
      boolean check = false;
      for (Pair pair_74 : searchIndex_74) {
        boolean flag = true;
        long[] times_74 = pair_74.times;
        if (times_74.length != times_72.length) {
          continue;
        }
        for (int i = 0; i < times_72.length; i++) {
          if (times_72[i] != times_74[i]) {
            flag = false;
            break;
          }
        }
        if (flag) {
          check = true;
          break;
        }
      }
      if (!check) {
        disappearedIndex = pair_72.searchIndex;
        System.out.println(disappearedIndex);
      }
    }

    for (Pair pair_72 : searchIndex_72) {
      if (pair_72.searchIndex == disappearedIndex) {
        System.out.println(pair_72.searchIndex);
      }
    }
  }

  private static Set<Pair> getSearchIndex(File[] walFiles) throws IOException {
    Arrays.sort(walFiles, new FileComparator());
    Set<Pair> insertTabletNodeSet = new HashSet<>();
    for (File walFile : walFiles) {
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          InsertTabletNode insertTabletNode = (InsertTabletNode) walReader.next().getValue();
          String path = insertTabletNode.getDevicePath().getNodes()[3];
          if (path.equals("d_530")) {
            Pair pair = new Pair(insertTabletNode.getTimes(), insertTabletNode.getSearchIndex());
            insertTabletNodeSet.add(pair);
          }
        }
      }
    }
    return insertTabletNodeSet;
  }

  private static Set<InsertTabletNode> getInsertTabletNodes(File[] walFiles) throws IOException {
    Arrays.sort(walFiles, new FileComparator());
    Set<InsertTabletNode> insertTabletNodeSet = new HashSet<>();
    for (File walFile : walFiles) {
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          InsertTabletNode insertTabletNode = (InsertTabletNode) walReader.next().getValue();
          String path = insertTabletNode.getDevicePath().getNodes()[3];
          if (path.equals("d_530")) {
            insertTabletNodeSet.add(insertTabletNode);
          }
        }
      }
    }
    return insertTabletNodeSet;
  }

  @Test
  public void processWALFileFrom72() throws Exception {
    File[] walFiles_72 = WALFileUtils.listAllWALFiles(new File(logDirectory_72));
    processInsertTabletNodes(walFiles_72, walBuffer_72);
    while (!walBuffer_72.isAllWALEntriesConsumed()) {
      Thread.sleep(1_000);
    }
  }

  @Test
  public void processWALFileFrom74() throws Exception {
    File[] walFiles_74 = WALFileUtils.listAllWALFiles(new File(logDirectory_74));
    processInsertTabletNodes(walFiles_74, walBuffer_74);
    while (!walBuffer_74.isAllWALEntriesConsumed()) {
      Thread.sleep(1_000);
    }
  }

  @Test
  public void processWALFileFrom75() throws Exception {
    File[] walFiles_75 = WALFileUtils.listAllWALFiles(new File(logDirectory_75));
    processInsertTabletNodes(walFiles_75, walBuffer_75);
    while (!walBuffer_75.isAllWALEntriesConsumed()) {
      Thread.sleep(1_000);
    }
  }

  private static void processInsertTabletNodes(File[] walFiles, IWALBuffer walBuffer)
      throws IOException {
    Arrays.sort(walFiles, new FileComparator());
    for (File walFile : walFiles) {
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          InsertTabletNode insertTabletNode = (InsertTabletNode) walReader.next().getValue();
          String path = insertTabletNode.getDevicePath().getNodes()[3];
          if (path.equals("d_353")) {
            WALEntry walEntry = new WALInfoEntry(1, insertTabletNode);
            walBuffer.write(walEntry);
          }
        }
      }
    }
  }

  static class FileComparator implements Comparator<File> {
    @Override
    public int compare(File file1, File file2) {
      String filename1 = file1.getName();
      String filename2 = file2.getName();

      long id1 = WALFileUtils.parseVersionId(filename1);
      long id2 = WALFileUtils.parseVersionId(filename2);

      return Long.compare(id1, id2);
    }
  }
}
