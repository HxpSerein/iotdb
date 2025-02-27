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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;

import org.apache.tsfile.utils.TimeDuration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DateTimeUtilsTest {

  private ZoneOffset zoneOffset;
  private ZoneId zoneId;
  // 1546413207689
  // 2019-01-02T15:13:27.689+08:00
  private final long timestamp = 1546413207689L;
  private long delta;

  /** Test convertDatetimeStrToLong() method with different time precision. */
  @Test
  public void convertDatetimeStrToLongTest1() {
    zoneOffset = ZonedDateTime.now().getOffset();
    zoneId = ZoneId.systemDefault();
    if (zoneOffset.toString().equals("Z")) {
      delta = 8 * 3600000;
    } else {
      delta = (8 - Long.parseLong(zoneOffset.toString().split(":")[0])) * 3600000;
    }
    testConvertDatetimeStrToLongWithoutMS(zoneOffset, zoneId, timestamp - 689 + delta);
    testConvertDatetimeStrToLongWithMS(zoneOffset, zoneId, timestamp + delta);
    testConvertDatetimeStrToLongWithMS2(zoneOffset, zoneId, timestamp - 9 + delta);
  }

  @Test
  public void convertDatetimeStrToLongTest2() {
    zoneOffset = ZoneOffset.UTC;
    zoneId = ZoneId.of("Etc/UTC");
    delta = 8 * 3600000;
    testConvertDatetimeStrToLongWithoutMS(zoneOffset, zoneId, timestamp - 689 + delta);
    testConvertDatetimeStrToLongWithMS(zoneOffset, zoneId, timestamp + delta);
    testConvertDatetimeStrToLongWithMS2(zoneOffset, zoneId, timestamp - 9 + delta);
  }

  @Test
  public void convertDatetimeStrToLongTest3() {
    zoneOffset = ZoneOffset.UTC;
    zoneId = ZoneId.of("Etc/UTC");
    delta = 8 * 3600000;
    // 2019-01-02T00:00:00.000+08:00
    long timestamp1 = 1546358400000L;
    testConvertDateStrToLong(zoneOffset, zoneId, timestamp1 + delta);
  }

  @Test
  public void convertDatetimeStrToLongTest4() {
    zoneOffset = ZoneOffset.UTC;
    try {
      DateTimeUtils.convertDatetimeStrToLong("1999-02-29T00:00:00.000", zoneOffset, 0, "ms");
      fail();
    } catch (Exception e) {
      assertEquals(
          "Text '1999-02-29T00:00:00.000+00:00' could not be parsed: Invalid date 'February 29' as '1999' is not a leap year",
          e.getMessage());
    }
  }

  /** Test time precision is ms. */
  @Test
  public void convertDurationStrToLongTest1() {
    Assert.assertEquals(7000L, DateTimeUtils.convertDurationStrToLongForTest(7, "s", "ms"));
    Assert.assertEquals(420000L, DateTimeUtils.convertDurationStrToLongForTest(7, "m", "ms"));
    Assert.assertEquals(25200000L, DateTimeUtils.convertDurationStrToLongForTest(7, "h", "ms"));
    Assert.assertEquals(604800000L, DateTimeUtils.convertDurationStrToLongForTest(7, "d", "ms"));
    Assert.assertEquals(4233600000L, DateTimeUtils.convertDurationStrToLongForTest(7, "w", "ms"));
    Assert.assertEquals(18144000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "mo", "ms"));
    Assert.assertEquals(220752000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "y", "ms"));
    Assert.assertEquals(7L, DateTimeUtils.convertDurationStrToLongForTest(7, "ms", "ms"));
    Assert.assertEquals(7L, DateTimeUtils.convertDurationStrToLongForTest(7000, "us", "ms"));
    Assert.assertEquals(7L, DateTimeUtils.convertDurationStrToLongForTest(7000000, "ns", "ms"));
  }

  /** Test time precision is us. */
  @Test
  public void convertDurationStrToLongTest2() {
    Assert.assertEquals(7000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "s", "us"));
    Assert.assertEquals(420000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "m", "us"));
    Assert.assertEquals(25200000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "h", "us"));
    Assert.assertEquals(604800000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "d", "us"));
    Assert.assertEquals(
        4233600000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "w", "us"));
    Assert.assertEquals(
        18144000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "mo", "us"));
    Assert.assertEquals(
        220752000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "y", "us"));
    Assert.assertEquals(7000L, DateTimeUtils.convertDurationStrToLongForTest(7, "ms", "us"));
    Assert.assertEquals(7L, DateTimeUtils.convertDurationStrToLongForTest(7, "us", "us"));
    Assert.assertEquals(7L, DateTimeUtils.convertDurationStrToLongForTest(7000, "ns", "us"));
  }

  /** Test time precision is ns. */
  @Test
  public void convertDurationStrToLongTest3() {
    Assert.assertEquals(7000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "s", "ns"));
    Assert.assertEquals(420000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "m", "ns"));
    Assert.assertEquals(
        25200000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "h", "ns"));
    Assert.assertEquals(
        604800000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "d", "ns"));
    Assert.assertEquals(
        4233600000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "w", "ns"));
    Assert.assertEquals(
        18144000000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "mo", "ns"));
    Assert.assertEquals(
        220752000000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "y", "ns"));
    Assert.assertEquals(7000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "ms", "ns"));
    Assert.assertEquals(7000L, DateTimeUtils.convertDurationStrToLongForTest(7, "us", "ns"));
    Assert.assertEquals(7L, DateTimeUtils.convertDurationStrToLongForTest(7, "ns", "ns"));
  }

  @Test
  public void getInstantWithPrecisionTest() {
    Assert.assertEquals(7000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "s", "ns"));
    Assert.assertEquals(420000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "m", "ns"));
    Assert.assertEquals(
        25200000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "h", "ns"));
    Assert.assertEquals(
        604800000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "d", "ns"));
    Assert.assertEquals(
        4233600000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "w", "ns"));
    Assert.assertEquals(
        18144000000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "mo", "ns"));
    Assert.assertEquals(
        220752000000000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "y", "ns"));
    Assert.assertEquals(7000000L, DateTimeUtils.convertDurationStrToLongForTest(7, "ms", "ns"));
    Assert.assertEquals(7000L, DateTimeUtils.convertDurationStrToLongForTest(7, "us", "ns"));
    Assert.assertEquals(7L, DateTimeUtils.convertDurationStrToLongForTest(7, "ns", "ns"));
  }

  @Ignore
  /** Test convert duration including natural month unit. Time includes: 1970-01-01 ~ 1970-12-01 */
  @Test
  public void getConvertDurationIncludingMonthUnit() {
    // force the current session's timezone to be UTC
    IClientSession session = new InternalClientSession("getConvertDurationIncludingMonthUnit");
    session.setZoneId(ZoneId.of("UTC"));

    try {
      SessionManager.getInstance().registerSession(session);

      Assert.assertEquals(31 * 86400000L, DateTimeUtils.convertDurationStrToLong(0, 1, "mo", "ms"));
      Assert.assertEquals(
          28 * 86400000L, DateTimeUtils.convertDurationStrToLong(2678400000L, 1, "mo", "ms"));
      TimeZone.getTimeZone(ZoneOffset.UTC);
      Assert.assertEquals(
          31 * 86400000L, DateTimeUtils.convertDurationStrToLong(5097600000L, 1, "mo", "ms"));
      Assert.assertEquals(
          30 * 86400000L, DateTimeUtils.convertDurationStrToLong(7776000000L, 1, "mo", "ms"));
      Assert.assertEquals(
          31 * 86400000L, DateTimeUtils.convertDurationStrToLong(10368000000L, 1, "mo", "ms"));
      Assert.assertEquals(
          30 * 86400000L, DateTimeUtils.convertDurationStrToLong(13046400000L, 1, "mo", "ms"));
      Assert.assertEquals(
          31 * 86400000L, DateTimeUtils.convertDurationStrToLong(15638400000L, 1, "mo", "ms"));
      Assert.assertEquals(
          31 * 86400000L, DateTimeUtils.convertDurationStrToLong(18316800000L, 1, "mo", "ms"));
      Assert.assertEquals(
          30 * 86400000L, DateTimeUtils.convertDurationStrToLong(20995200000L, 1, "mo", "ms"));
      Assert.assertEquals(
          31 * 86400000L, DateTimeUtils.convertDurationStrToLong(23587200000L, 1, "mo", "ms"));
      Assert.assertEquals(
          30 * 86400000L, DateTimeUtils.convertDurationStrToLong(26265600000L, 1, "mo", "ms"));
      Assert.assertEquals(
          31 * 86400000L, DateTimeUtils.convertDurationStrToLong(28857600000L, 1, "mo", "ms"));
    } finally {
      // clean up the session after test
      SessionManager.getInstance().removeCurrSession();
    }
  }

  public void testConvertDatetimeStrToLongWithoutMS(
      ZoneOffset zoneOffset, ZoneId zoneId, long res) {
    String[] timeFormatWithoutMs =
        new String[] {
          "2019-01-02 15:13:27",
          "2019/01/02 15:13:27",
          "2019.01.02 15:13:27",
          "2019-01-02T15:13:27",
          "2019/01/02T15:13:27",
          "2019.01.02T15:13:27",
          "2019-01-02 15:13:27" + zoneOffset,
          "2019/01/02 15:13:27" + zoneOffset,
          "2019.01.02 15:13:27" + zoneOffset,
          "2019-01-02T15:13:27" + zoneOffset,
          "2019/01/02T15:13:27" + zoneOffset,
          "2019.01.02T15:13:27" + zoneOffset,
        };
    for (String str : timeFormatWithoutMs) {
      Assert.assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneOffset, 0, "ms"));
    }

    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneId));
    }
  }

  public void testConvertDatetimeStrToLongWithMS(ZoneOffset zoneOffset, ZoneId zoneId, long res) {
    String[] timeFormatWithoutMs =
        new String[] {
          "2019-01-02 15:13:27.689",
          "2019/01/02 15:13:27.689",
          "2019.01.02 15:13:27.689",
          "2019-01-02T15:13:27.689",
          "2019/01/02T15:13:27.689",
          "2019.01.02T15:13:27.689",
          "2019-01-02 15:13:27.689" + zoneOffset,
          "2019/01/02 15:13:27.689" + zoneOffset,
          "2019.01.02 15:13:27.689" + zoneOffset,
          "2019-01-02T15:13:27.689" + zoneOffset,
          "2019/01/02T15:13:27.689" + zoneOffset,
          "2019.01.02T15:13:27.689" + zoneOffset,
        };
    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneOffset, 0, "ms"));
    }

    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneId));
    }
  }

  public void testConvertDatetimeStrToLongWithMS2(ZoneOffset zoneOffset, ZoneId zoneId, long res) {
    String[] timeFormatWithoutMs =
        new String[] {
          "2019-01-02 15:13:27.680",
          "2019/01/02 15:13:27.680",
          "2019.01.02 15:13:27.680",
          "2019-01-02T15:13:27.680",
          "2019-01-02T15:13:27.680",
          "2019/01/02T15:13:27.680",
          "2019.01.02T15:13:27.680",
          "2019-01-02 15:13:27.680" + zoneOffset,
          "2019/01/02 15:13:27.680" + zoneOffset,
          "2019.01.02 15:13:27.680" + zoneOffset,
          "2019-01-02T15:13:27.680" + zoneOffset,
          "2019/01/02T15:13:27.680" + zoneOffset,
          "2019.01.02T15:13:27.680" + zoneOffset,
          "2019-01-02 15:13:27.68",
          "2019/01/02 15:13:27.68",
          "2019.01.02 15:13:27.68",
          "2019-01-02T15:13:27.68",
          "2019-01-02T15:13:27.68",
          "2019/01/02T15:13:27.68",
          "2019.01.02T15:13:27.68",
          "2019-01-02 15:13:27.68" + zoneOffset,
          "2019/01/02 15:13:27.68" + zoneOffset,
          "2019.01.02 15:13:27.68" + zoneOffset,
          "2019-01-02T15:13:27.68" + zoneOffset,
          "2019/01/02T15:13:27.68" + zoneOffset,
          "2019.01.02T15:13:27.68" + zoneOffset,
        };
    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneOffset, 0, "ms"));
    }

    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneId));
    }
  }

  public void testConvertDateStrToLong(ZoneOffset zoneOffset, ZoneId zoneId, long res) {
    String[] timeFormatWithoutMs =
        new String[] {
          "2019-01-02", "2019/01/02", "2019.01.02",
        };
    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneOffset, 0, "ms"));
    }

    for (String str : timeFormatWithoutMs) {
      assertEquals(res, DateTimeUtils.convertDatetimeStrToLong(str, zoneId));
    }
  }

  @Test
  public void testConstructTimeDuration() {
    TimeDuration timeDuration = DateTimeUtils.constructTimeDuration("1y1d1ns");
    Assert.assertEquals(12, timeDuration.monthDuration);
    Assert.assertEquals(86400_000L, timeDuration.nonMonthDuration);

    timeDuration = DateTimeUtils.constructTimeDuration("1y1mo1d1ms1ns");
    Assert.assertEquals(13, timeDuration.monthDuration);
    Assert.assertEquals(86400_001L, timeDuration.nonMonthDuration);

    timeDuration = DateTimeUtils.constructTimeDuration("1d1ns");
    Assert.assertEquals(0, timeDuration.monthDuration);
    Assert.assertEquals(86400_000L, timeDuration.nonMonthDuration);

    timeDuration = DateTimeUtils.constructTimeDuration("1y");
    Assert.assertEquals(12, timeDuration.monthDuration);
    Assert.assertEquals(0, timeDuration.nonMonthDuration);

    timeDuration = DateTimeUtils.constructTimeDuration("1mo");
    Assert.assertEquals(1, timeDuration.monthDuration);
    Assert.assertEquals(0, timeDuration.nonMonthDuration);

    timeDuration = DateTimeUtils.constructTimeDuration("1y1mo");
    Assert.assertEquals(13, timeDuration.monthDuration);
    Assert.assertEquals(0, timeDuration.nonMonthDuration);

    timeDuration = DateTimeUtils.constructTimeDuration("10000000000ms");
    Assert.assertEquals(10000000000L, timeDuration.nonMonthDuration);
  }
}
