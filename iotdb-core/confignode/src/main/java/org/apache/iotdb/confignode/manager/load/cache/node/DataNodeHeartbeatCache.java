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

package org.apache.iotdb.confignode.manager.load.cache.node;

import org.apache.iotdb.common.rpc.thrift.TLoadSample;
import org.apache.iotdb.commons.cluster.NodeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/** Heartbeat cache for cluster DataNodes. */
public class DataNodeHeartbeatCache extends BaseNodeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeHeartbeatCache.class);

  // TODO: The load sample may be moved into NodeStatistics in the future
  private final AtomicReference<TLoadSample> latestLoadSample;

  /** Constructor for create DataNodeHeartbeatCache with default NodeStatistics. */
  public DataNodeHeartbeatCache(int dataNodeId) {
    super(dataNodeId);
    this.latestLoadSample = new AtomicReference<>(new TLoadSample());
  }

  @Override
  public synchronized void updateCurrentStatistics(boolean forceUpdate) {
    // The Removing status can not be updated
    if (!forceUpdate && NodeStatus.Removing.equals(getNodeStatus())) {
      return;
    }

    NodeHeartbeatSample lastSample;
    synchronized (slidingWindow) {
      lastSample = (NodeHeartbeatSample) getLastSample();
    }
    long lastSendTime = lastSample == null ? 0 : lastSample.getSampleLogicalTimestamp();

    /* Update load sample */
    if (lastSample != null && lastSample.isSetLoadSample()) {
      latestLoadSample.set(lastSample.getLoadSample());
    }

    /* Update Node status */
    NodeStatus status;
    String statusReason = null;
    long currentNanoTime = System.nanoTime();
    if (lastSample == null) {
      status = NodeStatus.Unknown;
    } else if (currentNanoTime - lastSendTime > HEARTBEAT_TIMEOUT_TIME_IN_NS) {
      // TODO: Optimize Unknown judge logic
      status = NodeStatus.Unknown;
    } else {
      status = lastSample.getStatus();
      statusReason = lastSample.getStatusReason();
    }

    /* Update loadScore */
    // Only consider Running DataNode as available currently
    // TODO: Construct load score module
    long loadScore = NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE;

    currentStatistics.set(new NodeStatistics(currentNanoTime, status, statusReason, loadScore));

    if (forceUpdate) {
      LOGGER.debug(
          "Force update NodeCache: status={}, currentNanoTime={}", status, currentNanoTime);
    }
  }

  public double getFreeDiskSpace() {
    return latestLoadSample.get().getFreeDiskSpace();
  }
}
