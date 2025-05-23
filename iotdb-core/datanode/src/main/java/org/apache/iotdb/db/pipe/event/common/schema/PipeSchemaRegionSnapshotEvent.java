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

package org.apache.iotdb.db.pipe.event.common.schema;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.commons.pipe.resource.snapshot.PipeSnapshotResourceManager;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PipeSchemaRegionSnapshotEvent extends PipeSnapshotEvent
    implements ReferenceTrackableEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSchemaRegionSnapshotEvent.class);
  private String mTreeSnapshotPath;
  private String tagLogSnapshotPath;
  private String attributeSnapshotPath;
  private String databaseName;

  private int version = 2;

  private static final Map<Short, StatementType> PLAN_NODE_2_STATEMENT_TYPE_MAP = new HashMap<>();

  static {
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.CREATE_TIME_SERIES.getNodeType(), StatementType.CREATE_TIME_SERIES);
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.CREATE_ALIGNED_TIME_SERIES.getNodeType(),
        StatementType.CREATE_ALIGNED_TIME_SERIES);
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.INTERNAL_CREATE_MULTI_TIMESERIES.getNodeType(),
        StatementType.INTERNAL_CREATE_MULTI_TIMESERIES);

    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.ACTIVATE_TEMPLATE.getNodeType(), StatementType.ACTIVATE_TEMPLATE);
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.BATCH_ACTIVATE_TEMPLATE.getNodeType(), StatementType.BATCH_ACTIVATE_TEMPLATE);

    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.CREATE_LOGICAL_VIEW.getNodeType(), StatementType.CREATE_LOGICAL_VIEW);
    // For logical view
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.ALTER_LOGICAL_VIEW.getNodeType(), StatementType.ALTER_LOGICAL_VIEW);

    // The table node does not have Statement type, use "auto_create_device" instead
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.CREATE_OR_UPDATE_TABLE_DEVICE.getNodeType(),
        StatementType.AUTO_CREATE_DEVICE_MNODE);
  }

  public PipeSchemaRegionSnapshotEvent(final int version) {
    // Used for deserialization
    this(null, null, null, null);
    this.version = version;
  }

  public PipeSchemaRegionSnapshotEvent(
      final String mTreeSnapshotPath,
      final String tagLogSnapshotPath,
      final String attributeSnapshotPath,
      final String databaseName) {
    this(
        mTreeSnapshotPath,
        tagLogSnapshotPath,
        attributeSnapshotPath,
        databaseName,
        null,
        0,
        null,
        null,
        null,
        null,
        true);
  }

  public PipeSchemaRegionSnapshotEvent(
      final String mTreeSnapshotPath,
      final String tagLogSnapshotPath,
      final String attributeSnapshotPath,
      final String databaseName,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userName,
      final boolean skipIfNoPrivileges) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        userName,
        skipIfNoPrivileges,
        PipeDataNodeResourceManager.snapshot());
    this.mTreeSnapshotPath = mTreeSnapshotPath;
    this.tagLogSnapshotPath = Objects.nonNull(tagLogSnapshotPath) ? tagLogSnapshotPath : "";
    this.attributeSnapshotPath =
        Objects.nonNull(attributeSnapshotPath) ? attributeSnapshotPath : "";
    this.databaseName = databaseName;
  }

  public File getMTreeSnapshotFile() {
    return new File(mTreeSnapshotPath);
  }

  public File getTagLogSnapshotFile() {
    return !tagLogSnapshotPath.isEmpty() ? new File(tagLogSnapshotPath) : null;
  }

  public File getAttributeSnapshotFile() {
    return !attributeSnapshotPath.isEmpty() ? new File(attributeSnapshotPath) : null;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    try {
      mTreeSnapshotPath = resourceManager.increaseSnapshotReference(mTreeSnapshotPath);
      if (!tagLogSnapshotPath.isEmpty()) {
        tagLogSnapshotPath = resourceManager.increaseSnapshotReference(tagLogSnapshotPath);
      }
      if (!attributeSnapshotPath.isEmpty()) {
        attributeSnapshotPath = resourceManager.increaseSnapshotReference(attributeSnapshotPath);
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for mTree snapshot %s or tLog %s error. Holder Message: %s",
              mTreeSnapshotPath, tagLogSnapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    try {
      resourceManager.decreaseSnapshotReference(mTreeSnapshotPath);
      if (!tagLogSnapshotPath.isEmpty()) {
        resourceManager.decreaseSnapshotReference(tagLogSnapshotPath);
      }
      if (!attributeSnapshotPath.isEmpty()) {
        resourceManager.decreaseSnapshotReference(attributeSnapshotPath);
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for mTree snapshot %s or tLog %s error. Holder Message: %s",
              mTreeSnapshotPath, tagLogSnapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userName,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
    return new PipeSchemaRegionSnapshotEvent(
        mTreeSnapshotPath,
        tagLogSnapshotPath,
        attributeSnapshotPath,
        databaseName,
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        userName,
        skipIfNoPrivileges);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    final ByteBuffer result =
        ByteBuffer.allocate(
            Byte.BYTES
                + 4 * Integer.BYTES
                + mTreeSnapshotPath.getBytes().length
                + tagLogSnapshotPath.getBytes().length
                + attributeSnapshotPath.getBytes().length
                + databaseName.getBytes().length);
    ReadWriteIOUtils.write(PipeSchemaSerializableEventType.SCHEMA_SNAPSHOT_V2.getType(), result);
    ReadWriteIOUtils.write(mTreeSnapshotPath, result);
    ReadWriteIOUtils.write(tagLogSnapshotPath, result);
    ReadWriteIOUtils.write(attributeSnapshotPath, result);
    ReadWriteIOUtils.write(databaseName, result);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(final ByteBuffer buffer) {
    mTreeSnapshotPath = ReadWriteIOUtils.readString(buffer);
    tagLogSnapshotPath = ReadWriteIOUtils.readString(buffer);
    attributeSnapshotPath = version >= 2 ? ReadWriteIOUtils.readString(buffer) : null;
    databaseName = ReadWriteIOUtils.readString(buffer);
  }

  /////////////////////////////// Type parsing ///////////////////////////////

  public static boolean needTransferSnapshot(final Set<PlanNodeType> listenedTypeSet) {
    final Set<Short> types = new HashSet<>(PLAN_NODE_2_STATEMENT_TYPE_MAP.keySet());
    types.retainAll(
        listenedTypeSet.stream().map(PlanNodeType::getNodeType).collect(Collectors.toSet()));
    return !types.isEmpty();
  }

  public void confineTransferredTypes(final Set<PlanNodeType> listenedTypeSet) {
    final Set<Short> types = new HashSet<>(PLAN_NODE_2_STATEMENT_TYPE_MAP.keySet());
    types.retainAll(
        listenedTypeSet.stream().map(PlanNodeType::getNodeType).collect(Collectors.toSet()));
    transferredTypes = types;
  }

  public static Set<StatementType> getStatementTypeSet(final String sealTypes) {
    final Map<Short, StatementType> statementTypeMap =
        new HashMap<>(PLAN_NODE_2_STATEMENT_TYPE_MAP);
    statementTypeMap
        .keySet()
        .retainAll(
            Arrays.stream(sealTypes.split(",")).map(Short::valueOf).collect(Collectors.toSet()));
    return new HashSet<>(statementTypeMap.values());
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeSchemaRegionSnapshotEvent{mTreeSnapshotPath=%s, tagLogSnapshotPath=%s, attributeSnapshotPath=%s, databaseName=%s}",
            mTreeSnapshotPath, tagLogSnapshotPath, attributeSnapshotPath, databaseName)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeSchemaRegionSnapshotEvent{mTreeSnapshotPath=%s, tagLogSnapshotPath=%s, attributeSnapshotPath=%s, databaseName=%s}",
            mTreeSnapshotPath, tagLogSnapshotPath, attributeSnapshotPath, databaseName)
        + " - "
        + super.coreReportMessage();
  }

  /////////////////////////// ReferenceTrackableEvent ///////////////////////////

  @Override
  protected void trackResource() {
    PipeDataNodeResourceManager.ref().trackPipeEventResource(this, eventResourceBuilder());
  }

  @Override
  public PipeEventResource eventResourceBuilder() {
    return new PipeSchemaRegionSnapshotEventResource(
        this.isReleased,
        this.referenceCount,
        this.resourceManager,
        this.mTreeSnapshotPath,
        this.tagLogSnapshotPath,
        this.attributeSnapshotPath);
  }

  private static class PipeSchemaRegionSnapshotEventResource extends PipeEventResource {

    private final PipeSnapshotResourceManager resourceManager;
    private final String mTreeSnapshotPath;
    private final String tagLogSnapshotPath;
    private final String attributeSnapshotPath;

    private PipeSchemaRegionSnapshotEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final PipeSnapshotResourceManager resourceManager,
        final String mTreeSnapshotPath,
        final String tagLogSnapshotPath,
        final String attributeSnapshotPath) {
      super(isReleased, referenceCount);
      this.resourceManager = resourceManager;
      this.mTreeSnapshotPath = mTreeSnapshotPath;
      this.tagLogSnapshotPath = tagLogSnapshotPath;
      this.attributeSnapshotPath = attributeSnapshotPath;
    }

    @Override
    protected void finalizeResource() {
      try {
        resourceManager.decreaseSnapshotReference(mTreeSnapshotPath);
        if (!tagLogSnapshotPath.isEmpty()) {
          resourceManager.decreaseSnapshotReference(tagLogSnapshotPath);
        }
        if (!attributeSnapshotPath.isEmpty()) {
          resourceManager.decreaseSnapshotReference(attributeSnapshotPath);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Decrease reference count for mTree snapshot {} or tLog {} or attribute snapshot {} error.",
            mTreeSnapshotPath,
            tagLogSnapshotPath,
            attributeSnapshotPath,
            e);
      }
    }
  }
}
