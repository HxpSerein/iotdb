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

package org.apache.iotdb.confignode.procedure.entity;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class IncProcedure extends Procedure<TestProcEnv> {

  public static final Logger LOGGER = LoggerFactory.getLogger(IncProcedure.class);

  public boolean throwEx = false;

  @Override
  protected Procedure<TestProcEnv>[] execute(TestProcEnv testProcEnv)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    LOGGER.info("IncProcedure {} execute start", getProcId());
    AtomicInteger acc = testProcEnv.getAcc();
    if (throwEx) {
      LOGGER.info("IncProcedure {} throw exception", getProcId());
      setFailure(new ProcedureException("exception in IncProcedure"));
      throw new RuntimeException("throw a EXCEPTION");
    }
    // TimeUnit.SECONDS.sleep(5);
    acc.getAndIncrement();
    testProcEnv.successCount.getAndIncrement();
    LOGGER.info("IncProcedure {} execute successfully, acc: {}", getProcId(), acc.get());
    return null;
  }

  @Override
  protected void rollback(TestProcEnv testProcEnv) {
    LOGGER.info("IncProcedure {} start rollback", getProcId());
    AtomicInteger acc = testProcEnv.getAcc();
    acc.getAndDecrement();
    testProcEnv.rolledBackCount.getAndIncrement();
    LOGGER.info("IncProcedure {} end rollback successfully, acc: {}", getProcId(), acc.get());
  }

  @Override
  protected boolean abort(TestProcEnv testProcEnv) {
    return true;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(TestProcedureFactory.TestProcedureType.INC_PROCEDURE.ordinal());
    super.serialize(stream);
  }

  @Override
  protected ProcedureLockState acquireLock(TestProcEnv testProcEnv) {
    testProcEnv.getEnvLock().lock();
    try {
      if (testProcEnv.getExecuteLock().tryLock(this)) {
        LOGGER.info("IntProcedureId {} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      testProcEnv.getExecuteLock().waitProcedure(this);

      LOGGER.info("IntProcedureId {} wait for lock.", getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      testProcEnv.getEnvLock().unlock();
    }
  }

  @Override
  protected void releaseLock(TestProcEnv testProcEnv) {
    testProcEnv.getEnvLock().lock();
    try {
      LOGGER.info("IntProcedureId {} release lock.", getProcId());
      if (testProcEnv.getExecuteLock().releaseLock(this)) {
        testProcEnv.getExecuteLock().wakeWaitingProcedures(testProcEnv.getScheduler());
      }
    } finally {
      testProcEnv.getEnvLock().unlock();
    }
  }
}
