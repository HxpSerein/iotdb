package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.entity.FirstStateMachineProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class TestFirstStateMachineProcedure extends TestProcedureBase {

  public static final Logger LOGGER = LoggerFactory.getLogger(TestFirstStateMachineProcedure.class);

  @Test
  public void testRolledBackProcedure() {
    FirstStateMachineProcedure firstProcedure = new FirstStateMachineProcedure();
    firstProcedure.throwAtIndex = 3;
    long procId = this.procExecutor.submitProcedure(firstProcedure);
    ProcedureTestUtil.waitForProcedure(this.procExecutor, procId);
    TestProcEnv env = this.getEnv();
    AtomicInteger acc = env.getAcc();
    int successCount = env.successCount.get();
    int rolledBackCount = env.rolledBackCount.get();
    LOGGER.info("all count: " + acc.get());
    LOGGER.info("success count: " + successCount);
    LOGGER.info("rolledback count: " + rolledBackCount);
    Assert.assertEquals(1 + successCount - rolledBackCount, acc.get());
  }
}
