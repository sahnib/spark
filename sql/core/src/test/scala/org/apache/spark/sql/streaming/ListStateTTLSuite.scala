/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming

import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.{ListStateImplWithTTL, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock


class ListStateTTLProcessor
  extends StatefulProcessor[String, InputEvent, OutputEvent]
    with Logging {

  @transient private var _listState: ListStateImplWithTTL[Int] = _
  @transient private var _ttlMode: TTLMode = _

  override def init(
     outputMode: OutputMode,
     timeoutMode: TimeoutMode,
     ttlMode: TTLMode): Unit = {
    _listState = getHandle
      .getListState("listState", Encoders.scalaInt)
      .asInstanceOf[ListStateImplWithTTL[Int]]
    _ttlMode = ttlMode
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputEvent],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()

    for (row <- inputRows) {
      val resultIter = processRow(_ttlMode, row, _listState)
      resultIter.foreach { r =>
        results = r :: results
      }
    }

    results.iterator
  }


  def processRow(
      ttlMode: TTLMode,
      row: InputEvent,
      listState: ListStateImplWithTTL[Int]): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()
    val key = row.key
    if (row.action == "get") {
      val currState = listState.get()
      currState.foreach { v =>
        results = OutputEvent(key, v, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_without_enforcing_ttl") {
      val currState = listState.getWithoutEnforcingTTL()
      currState.foreach { v =>
        results = OutputEvent(key, v, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_ttl_value_from_state") {
      val ttlExpirations = listState.getTTLValues()
      // get the ttlExpirations that are defined and not equal to -1
      ttlExpirations.filter(_.isDefined).foreach { ttlExpiration =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlExpiration.get) :: results
      }
    } else if (row.action == "put") {
      val rowArr = Array(row.value)
      if (ttlMode == TTLMode.EventTimeTTL() && row.eventTimeTtl != null) {
        listState.put(rowArr, row.eventTimeTtl.getTime)
      } else if (ttlMode == TTLMode.EventTimeTTL()) {
        listState.put(rowArr)
      } else {
        listState.put(rowArr, row.ttl)
      }
    } else if (row.action == "append") {
      if (ttlMode == TTLMode.EventTimeTTL() && row.eventTimeTtl != null) {
        listState.appendValue(row.value, row.eventTimeTtl.getTime)
      } else if (ttlMode == TTLMode.EventTimeTTL()) {
        listState.appendValue(row.value)
      } else {
        listState.appendValue(row.value, row.ttl)
      }
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = listState.getValuesInTTLState()
      ttlValues.foreach { v =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlValue = v) :: results
      }
    }

    results.iterator
  }
}


class ListStateTTLSuite extends TransformWithStateTTLSuiteBase {

  import testImplicits._
  override def getProcessor(): StatefulProcessor[String, InputEvent, OutputEvent] = {
    new ListStateTTLProcessor()
  }

  test("verify iterator works with expired values in middle of list - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          getProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        // Add three elements with duration of a minute
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        AdvanceManualClock(1 * 1000),
        AddData(inputStream, InputEvent("k1", "append", 2, Duration.ofMinutes(1))),
        AddData(inputStream, InputEvent("k1", "append", 3, Duration.ofMinutes(1))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Add three elements with a duration of 15 seconds
        AddData(inputStream, InputEvent("k1", "append", 4, Duration.ofSeconds(15))),
        AddData(inputStream, InputEvent("k1", "append", 5, Duration.ofSeconds(15))),
        AddData(inputStream, InputEvent("k1", "append", 6, Duration.ofSeconds(15))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Add three elements with a duration of a minute
        AddData(inputStream, InputEvent("k1", "append", 7, Duration.ofMinutes(1))),
        AddData(inputStream, InputEvent("k1", "append", 8, Duration.ofMinutes(1))),
        AddData(inputStream, InputEvent("k1", "append", 9, Duration.ofMinutes(1))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Advance clock to expire the middle three elements
        AdvanceManualClock(30 * 1000),
        // Get all elements in the list
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // Validate that the expired elements are not returned
        CheckNewAnswer(
          OutputEvent("k1", 1, isTTLValue = false, -1),
          OutputEvent("k1", 2, isTTLValue = false, -1),
          OutputEvent("k1", 3, isTTLValue = false, -1),
          OutputEvent("k1", 7, isTTLValue = false, -1),
          OutputEvent("k1", 8, isTTLValue = false, -1),
          OutputEvent("k1", 9, isTTLValue = false, -1)
        )
      )
    }
  }

  test("verify iterator works with expired values in beginning of list - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          getProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        // Add three elements with duration of a minute
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        AdvanceManualClock(1 * 1000),
        AddData(inputStream, InputEvent("k1", "append", 2, Duration.ofMinutes(1))),
        AddData(inputStream, InputEvent("k1", "append", 3, Duration.ofMinutes(1))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Add three elements with a duration of 2 minutes
        AddData(inputStream, InputEvent("k1", "append", 4, Duration.ofMinutes(2))),
        AddData(inputStream, InputEvent("k1", "append", 5, Duration.ofMinutes(2))),
        AddData(inputStream, InputEvent("k1", "append", 6, Duration.ofMinutes(2))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Advance clock to expire the middle three elements
        AdvanceManualClock(60 * 1000),
        // Get all elements in the list
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // Validate that the expired elements are not returned
        CheckNewAnswer(
          OutputEvent("k1", 4, isTTLValue = false, -1),
          OutputEvent("k1", 5, isTTLValue = false, -1),
          OutputEvent("k1", 6, isTTLValue = false, -1)
        )
      )
    }
  }

  test("verify iterator works with expired values in middle of list - event time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .withWatermark("eventTime", "1 second")
        .groupByKey(x => x.key)
        .transformWithState(
          getProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.EventTimeTTL())

      val eventTime1 = Timestamp.valueOf("2024-01-01 00:00:00")
      val eventTime2 = Timestamp.valueOf("2024-01-01 00:01:00")
      val eventTime3 = Timestamp.valueOf("2024-01-01 00:03:00")

      val ttlExpirationEarly = Timestamp.valueOf("2024-01-01 00:02:00")

      val ttlExpirationLate = Timestamp.valueOf("2024-01-01 00:05:00")

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        // Add three elements with duration of a minute
        AddData(inputStream, InputEvent("k1", "put", 1, null, eventTime1, ttlExpirationLate)),
        AdvanceManualClock(1 * 1000),
        AddData(inputStream, InputEvent("k1", "append", 2, null, eventTime1, ttlExpirationLate)),
        AddData(inputStream, InputEvent("k1", "append", 3, null, eventTime1, ttlExpirationLate)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Add three elements with a duration of 15 seconds
        AddData(inputStream, InputEvent("k1", "append", 4, null, eventTime2, ttlExpirationEarly)),
        AddData(inputStream, InputEvent("k1", "append", 5, null, eventTime2, ttlExpirationEarly)),
        AddData(inputStream, InputEvent("k1", "append", 6, null, eventTime2, ttlExpirationEarly)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Add three elements with a duration of a minute
        AddData(inputStream, InputEvent("k1", "append", 7, null, eventTime3, ttlExpirationLate)),
        AddData(inputStream, InputEvent("k1", "append", 8, null, eventTime3, ttlExpirationLate)),
        AddData(inputStream, InputEvent("k1", "append", 9, null, eventTime3, ttlExpirationLate)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Advance clock to expire the middle three elements
        AdvanceManualClock(30 * 1000),
        // Get all elements in the list
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // Validate that the expired elements are not returned
        CheckNewAnswer(
          OutputEvent("k1", 1, isTTLValue = false, -1),
          OutputEvent("k1", 2, isTTLValue = false, -1),
          OutputEvent("k1", 3, isTTLValue = false, -1),
          OutputEvent("k1", 7, isTTLValue = false, -1),
          OutputEvent("k1", 8, isTTLValue = false, -1),
          OutputEvent("k1", 9, isTTLValue = false, -1)
        )
      )
    }
  }

  test("verify iterator works with expired values in beginning of list - event time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .withWatermark("eventTime", "1 second")
        .groupByKey(x => x.key)
        .transformWithState(
          getProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.EventTimeTTL())

      val eventTime1 = Timestamp.valueOf("2024-01-01 00:00:00")
      val eventTime2 = Timestamp.valueOf("2024-01-01 00:03:00")

      val ttlExpirationEarly = Timestamp.valueOf("2024-01-01 00:02:00")
      val ttlExpirationLate = Timestamp.valueOf("2024-01-01 00:05:00")

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        // Add three elements with duration of a minute
        AddData(inputStream, InputEvent("k1", "put", 1, null, eventTime1, ttlExpirationEarly)),
        AdvanceManualClock(1 * 1000),
        AddData(inputStream, InputEvent("k1", "append", 2, null, eventTime1, ttlExpirationEarly)),
        AddData(inputStream, InputEvent("k1", "append", 3, null, eventTime1, ttlExpirationEarly)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // Add three elements with a duration of 2 minutes
        AddData(inputStream, InputEvent("k1", "append", 4, null, eventTime2, ttlExpirationLate)),
        AddData(inputStream, InputEvent("k1", "append", 5, null, eventTime2, ttlExpirationLate)),
        AddData(inputStream, InputEvent("k1", "append", 6, null, eventTime2, ttlExpirationLate)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // Validate that the expired elements are not returned
        CheckNewAnswer(
          OutputEvent("k1", 4, isTTLValue = false, -1),
          OutputEvent("k1", 5, isTTLValue = false, -1),
          OutputEvent("k1", 6, isTTLValue = false, -1)
        )
      )
    }
  }
}
