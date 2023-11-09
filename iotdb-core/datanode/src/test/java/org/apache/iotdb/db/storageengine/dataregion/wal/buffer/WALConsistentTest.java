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

  protected static final String logDirectory_73 =
      "/home/serein/inconsistent/data/73/root.test.g_0-3";
  protected static final String logDirectory_74 =
      "/home/serein/inconsistent/data/74/root.test.g_0-3";
  protected static final String logDirectory_75 =
      "/home/serein/inconsistent/data/75/root.test.g_0-3";

  protected static final String logDirectory_73_167 = "/home/serein/inconsistent/data/73/167";
  protected static final String logDirectory_74_167 = "/home/serein/inconsistent/data/74/167";
  protected static final String logDirectory_75_167 = "/home/serein/inconsistent/data/75/167";

  protected IWALBuffer walBuffer_73;
  protected IWALBuffer walBuffer_74;
  protected IWALBuffer walBuffer_75;

  @Before
  public void setUp() throws Exception {
    walBuffer_73 = new WALBuffer("73", logDirectory_73_167);
    walBuffer_74 = new WALBuffer("74", logDirectory_74_167);
    walBuffer_75 = new WALBuffer("75", logDirectory_75_167);
    config.setClusterMode(true);
  }

  @After
  public void tearDown() throws Exception {
    walBuffer_73.close();
    walBuffer_74.close();
    walBuffer_75.close();
    config.setClusterMode(preIsClusterMode);
  }

  @Test
  public void readWALFile() throws Exception {
    File[] walFiles_73_167 = WALFileUtils.listAllWALFiles(new File(logDirectory_73_167));
    int rowCount_73 = getRowCount(walFiles_73_167);
    System.out.println(rowCount_73);
    //    Set<InsertTabletNode> actualInsertTabletNodes_73_167 =
    // getInsertTabletNodes(walFiles_73_167);
    //    System.out.println(actualInsertTabletNodes_73_167.size());

    File[] walFiles_74_167 = WALFileUtils.listAllWALFiles(new File(logDirectory_74_167));
    int rowCount_74 = getRowCount(walFiles_74_167);
    System.out.println(rowCount_74);
    //    Set<InsertTabletNode> actualInsertTabletNodes_74_167 =
    // getInsertTabletNodes(walFiles_74_167);
    //    System.out.println(actualInsertTabletNodes_74_167.size());

    File[] walFiles_75_167 = WALFileUtils.listAllWALFiles(new File(logDirectory_75_167));
    int rowCount_75 = getRowCount(walFiles_75_167);
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
          if (path.equals("d_167")) {
            rowCount += insertTabletNode.getRowCount();
          }
        }
      }
    }
    return rowCount;
  }

  private static Set<InsertTabletNode> getInsertTabletNodes(File[] walFiles) throws IOException {
    Arrays.sort(walFiles, new FileComparator());
    Set<InsertTabletNode> insertTabletNodeSet = new HashSet<>();
    for (File walFile : walFiles) {
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          InsertTabletNode insertTabletNode = (InsertTabletNode) walReader.next().getValue();
          String path = insertTabletNode.getDevicePath().getNodes()[3];
          if (path.equals("d_167")) {
            insertTabletNodeSet.add(insertTabletNode);
          }
        }
      }
    }
    return insertTabletNodeSet;
  }

  @Test
  public void processWALFileFrom73() throws Exception {
    File[] walFiles_73 = WALFileUtils.listAllWALFiles(new File(logDirectory_73));
    processInsertTabletNodes(walFiles_73, walBuffer_73);
    while (!walBuffer_73.isAllWALEntriesConsumed()) {
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
          if (path.equals("d_167")) {
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
