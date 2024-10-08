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

package org.apache.iotdb.commons.schema.view.viewExpression.unary;

import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;

import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.tsfile.common.regexp.LikePattern.getEscapeCharacter;

public class LikeViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  private final String patternString;
  private final LikePattern pattern;

  private final boolean isNot;

  public LikeViewExpression(ViewExpression expression, String patternString, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.isNot = isNot;
    pattern = LikePattern.compile(patternString, getEscapeCharacter(Optional.of("\\")));
  }

  public LikeViewExpression(
      ViewExpression expression, String patternString, LikePattern pattern, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.pattern = pattern;
    this.isNot = isNot;
  }

  public LikeViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
    pattern = LikePattern.compile(patternString, getEscapeCharacter(Optional.of("\\")));
  }

  public LikeViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
    try {
      patternString = ReadWriteIOUtils.readString(inputStream);
      isNot = ReadWriteIOUtils.readBool(inputStream);
      pattern = LikePattern.compile(patternString, getEscapeCharacter(Optional.of("\\")));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLikeExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LIKE;
  }

  @Override
  public String toString(boolean isRoot) {
    String basicString =
        this.expression.toString(false) + (isNot ? "NOT LIKE " : "LIKE ") + this.patternString;
    if (isRoot) {
      return basicString;
    }
    return "( " + basicString + " )";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(patternString, byteBuffer);
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(patternString, stream);
    ReadWriteIOUtils.write(isNot, stream);
  }

  // endregion

  public String getPatternString() {
    return patternString;
  }

  public LikePattern getPattern() {
    return pattern;
  }

  public boolean isNot() {
    return isNot;
  }
}
