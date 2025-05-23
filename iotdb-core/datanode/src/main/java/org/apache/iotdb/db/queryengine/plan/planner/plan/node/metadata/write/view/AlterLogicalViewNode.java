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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AlterLogicalViewNode extends PlanNode {

  /**
   * A map from target path to source expression. Yht target path is the name of this logical view,
   * and the source expression is the data source of this view.
   */
  private final Map<PartialPath, ViewExpression> viewPathToSourceMap;

  public AlterLogicalViewNode(
      final PlanNodeId id, final Map<PartialPath, ViewExpression> viewPathToSourceMap) {
    super(id);
    this.viewPathToSourceMap = viewPathToSourceMap;
  }

  public Map<PartialPath, ViewExpression> getViewPathToSourceMap() {
    return viewPathToSourceMap;
  }

  // For pipe
  // TODO: Add auth check for source paths
  public TSStatus checkPermissionBeforeProcess(final String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    final List<PartialPath> targetPathList = new ArrayList<>(viewPathToSourceMap.keySet());
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            userName, targetPathList, PrivilegeType.WRITE_SCHEMA),
        targetPathList,
        PrivilegeType.WRITE_SCHEMA);
  }

  // region Interfaces in WritePlanNode or PlanNode
  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterLogicalView(this, context);
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(final PlanNode child) {
    // do nothing. this node should never have any child
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.ALTER_LOGICAL_VIEW;
  }

  @Override
  public PlanNode clone() {
    // TODO: CRTODO, complete this method
    throw new NotImplementedException("Clone of AlterLogicalNode is not implemented");
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final AlterLogicalViewNode that = (AlterLogicalViewNode) obj;
    return (this.getPlanNodeId().equals(that.getPlanNodeId())
        && Objects.equals(this.viewPathToSourceMap, that.viewPathToSourceMap));
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getPlanNodeId(), this.viewPathToSourceMap);
  }

  @Override
  public int allowedChildCount() {
    // this node should never have any child
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    // TODO: CRTODO, complete this method
    throw new NotImplementedException(
        "getOutputColumnNames of AlterLogicalViewNode is not implemented");
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.ALTER_LOGICAL_VIEW.serialize(byteBuffer);
    // serialize other member variables for this node
    ReadWriteIOUtils.write(this.viewPathToSourceMap.size(), byteBuffer);
    for (final Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      ViewExpression.serialize(entry.getValue(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.ALTER_LOGICAL_VIEW.serialize(stream);
    // serialize other member variables for this node
    ReadWriteIOUtils.write(this.viewPathToSourceMap.size(), stream);
    for (final Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      entry.getKey().serialize(stream);
      ViewExpression.serialize(entry.getValue(), stream);
    }
  }

  public static AlterLogicalViewNode deserialize(final ByteBuffer byteBuffer) {
    // deserialize member variables
    final Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    final int size = byteBuffer.getInt();
    PartialPath path;
    ViewExpression viewExpression;
    for (int i = 0; i < size; i++) {
      path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      viewExpression = ViewExpression.deserialize(byteBuffer);
      viewPathToSourceMap.put(path, viewExpression);
    }
    // deserialize PlanNodeId next
    final PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AlterLogicalViewNode(planNodeId, viewPathToSourceMap);
  }
  // endregion
}
