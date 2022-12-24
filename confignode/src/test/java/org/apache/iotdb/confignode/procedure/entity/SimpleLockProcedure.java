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
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SimpleLockProcedure extends Procedure<TestProcEnv> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLockProcedure.class);

  private String procName;

  public SimpleLockProcedure() {}

  public SimpleLockProcedure(String procName) {
    this.procName = procName;
  }

  @Override
  protected Procedure<TestProcEnv>[] execute(TestProcEnv testProcEnv)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    testProcEnv.executeSeq.append(procName);
    return null;
  }

  @Override
  protected void rollback(TestProcEnv testProcEnv) {}

  @Override
  protected boolean abort(TestProcEnv testProcEnv) {
    return false;
  }

  @Override
  protected ProcedureLockState acquireLock(TestProcEnv testProcEnv) {
    if (testProcEnv.getEnvLock().tryLock()) {
      testProcEnv.lockAcquireSeq.append(procName);
      LOGGER.info(procName + " acquired lock.");

      return ProcedureLockState.LOCK_ACQUIRED;
    }
    SimpleProcedureScheduler scheduler = (SimpleProcedureScheduler) testProcEnv.getScheduler();
    scheduler.addWaiting(this);
    LOGGER.info(procName + " wait for lock.");
    return ProcedureLockState.LOCK_EVENT_WAIT;
  }

  @Override
  protected void releaseLock(TestProcEnv testProcEnv) {
    LOGGER.info(procName + " release lock.");
    testProcEnv.getEnvLock().unlock();
    SimpleProcedureScheduler scheduler = (SimpleProcedureScheduler) testProcEnv.getScheduler();
    scheduler.releaseWaiting();
  }

  @Override
  protected boolean holdLock(TestProcEnv testProcEnv) {
    return testProcEnv.getEnvLock().isHeldByCurrentThread();
  }
}
