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
import java.util.HashSet;
import java.util.Set;

public class WALConsistentTest {
  protected static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected static final String identifier = String.valueOf(Integer.MAX_VALUE);
  protected static final boolean preIsClusterMode = config.isClusterMode();

  protected static final String logDirectory_36 =
      "C:/Users/serein/Downloads/72/36/wal/root.test.g_0-3";
  protected static final String logDirectory_37 =
      "C:/Users/serein/Downloads/72/37/wal/wal/root.test.g_0-3";
  protected static final String logDirectory_39 =
      "C:/Users/serein/Downloads/72/39/wal/root.test.g_0-3";

  protected static final String devicePath = "root.test_sg.test_d";
  protected IWALBuffer walBuffer_36;
  protected IWALBuffer walBuffer_37;
  protected IWALBuffer walBuffer_39;

  @Before
  public void setUp() throws Exception {
    walBuffer_36 = new WALBuffer(identifier, logDirectory_36);
    walBuffer_37 = new WALBuffer(identifier, logDirectory_37);
    walBuffer_39 = new WALBuffer(identifier, logDirectory_39);
    config.setClusterMode(true);
  }

  @After
  public void tearDown() throws Exception {
    walBuffer_36.close();
    walBuffer_37.close();
    walBuffer_39.close();
    config.setClusterMode(preIsClusterMode);
  }

  @Test
  public void readWALFile() throws Exception {
    File[] walFiles = WALFileUtils.listAllWALFiles(new File(logDirectory_37));
    Set<InsertTabletNode> actualInsertTabletNodes = new HashSet<>();
    if (walFiles != null) {
      for (File walFile : walFiles) {
        try (WALReader walReader = new WALReader(walFile)) {
          while (walReader.hasNext()) {
            actualInsertTabletNodes.add((InsertTabletNode) walReader.next().getValue());
          }
        }
      }
    }
    walFiles = WALFileUtils.listAllWALFiles(new File(logDirectory_37));
  }
}
