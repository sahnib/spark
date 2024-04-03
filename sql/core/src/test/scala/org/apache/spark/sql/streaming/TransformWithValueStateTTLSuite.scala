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
import org.apache.spark.sql.execution.streaming.{MemoryStream, ValueStateImplWithTTL}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

case class InputEvent(
    key: String,
    action: String,
    value: Int,
    ttl: Duration,
    eventTime: Timestamp = null,
    eventTimeTtl: Timestamp = null)

case class OutputEvent(
    key: String,
    value: Int,
    isTTLValue: Boolean,
    ttlValue: Long)

object TTLInputProcessFunction {
  def processRow(
      ttlMode: TTLMode,
      row: InputEvent,
      valueState: ValueStateImplWithTTL[Int]): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()
    val key = row.key
    if (row.action == "get") {
      val currState = valueState.getOption()
      if (currState.isDefined) {
        results = OutputEvent(key, currState.get, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_without_enforcing_ttl") {
      val currState = valueState.getWithoutEnforcingTTL()
      if (currState.isDefined) {
        results = OutputEvent(key, currState.get, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_ttl_value_from_state") {
      val ttlExpiration = valueState.getTTLValue()
      if (ttlExpiration.isDefined) {
        results = OutputEvent(key, -1, isTTLValue = true, ttlExpiration.get) :: results
      }
    } else if (row.action == "put") {
      if (ttlMode == TTLMode.EventTimeTTL() && row.eventTimeTtl != null) {
        valueState.update(row.value, row.eventTimeTtl.getTime)
      } else if (ttlMode == TTLMode.EventTimeTTL()) {
        valueState.update(row.value)
      } else {
        valueState.update(row.value, row.ttl)
      }
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = valueState.getValuesInTTLState()
      ttlValues.foreach { v =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlValue = v) :: results
      }
    }

    results.iterator
  }
}

class ValueStateTTLProcessor
  extends StatefulProcessor[String, InputEvent, OutputEvent]
  with Logging {

  @transient private var _valueState: ValueStateImplWithTTL[Int] = _
  @transient private var _ttlMode: TTLMode = _

  override def init(
      outputMode: OutputMode,
      timeoutMode: TimeoutMode,
      ttlMode: TTLMode): Unit = {
    _valueState = getHandle
      .getValueState("valueState", Encoders.scalaInt)
      .asInstanceOf[ValueStateImplWithTTL[Int]]
    _ttlMode = ttlMode
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputEvent],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()

    inputRows.foreach { row =>
      val resultIter = TTLInputProcessFunction.processRow(_ttlMode, row, _valueState)
      resultIter.foreach { r =>
        results = r :: results
      }
    }

    results.iterator
  }
}

case class MultipleValueStatesTTLProcessor(
    ttlKey: String,
    noTtlKey: String)
  extends StatefulProcessor[String, InputEvent, OutputEvent]
    with Logging {

  @transient private var _valueStateWithTTL: ValueStateImplWithTTL[Int] = _
  @transient private var _valueStateWithoutTTL: ValueStateImplWithTTL[Int] = _
  @transient private var _ttlMode: TTLMode = _

  override def init(
      outputMode: OutputMode,
      timeoutMode: TimeoutMode,
      ttlMode: TTLMode): Unit = {
    _valueStateWithTTL = getHandle
      .getValueState("valueState", Encoders.scalaInt)
      .asInstanceOf[ValueStateImplWithTTL[Int]]
    _valueStateWithoutTTL = getHandle
      .getValueState("valueState", Encoders.scalaInt)
      .asInstanceOf[ValueStateImplWithTTL[Int]]
    _ttlMode = ttlMode
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputEvent],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()
    val state = if (key == ttlKey) {
      _valueStateWithTTL
    } else {
      _valueStateWithoutTTL
    }

    inputRows.foreach { row =>
      val resultIterator = TTLInputProcessFunction.processRow(_ttlMode, row, state)
      resultIterator.foreach { r =>
        results = r :: results
      }
    }
    results.iterator
  }
}

/**
 * Tests that ttl works as expected for Value State for
 * processing time and event time based ttl.
 */
class TransformWithValueStateTTLSuite
  extends StreamTest {
  import testImplicits._

  test("validate state is evicted at ttl expiry - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { dir =>
        val inputStream = MemoryStream[InputEvent]
        val result = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            new ValueStateTTLProcessor(),
            TimeoutMode.NoTimeouts(),
            TTLMode.ProcessingTimeTTL(),
            OutputMode.Append())

        val clock = new StreamManualClock
        testStream(result)(
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          StopStream,
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          // get this state, and make sure we get unexpired value
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
          StopStream,
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          // ensure ttl values were added correctly
          AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
          StopStream,
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          // advance clock so that state expires
          AdvanceManualClock(60 * 1000),
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          // validate expired value is not returned
          CheckNewAnswer(),
          // ensure this state does not exist any longer in State
          AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer()
        )
      }
    }
  }

  test("validate ttl update updates the expiration timestamp - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        // advance clock and update expiration time
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        // validate value is not expired
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // validate ttl value is updated in the state
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 95000)),
        // validate ttl state has both ttl values present
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000),
          OutputEvent("k1", -1, isTTLValue = true, 95000)
        ),
        // advance clock after older expiration value
        AdvanceManualClock(30 * 1000),
        // ensure unexpired value is still present in the state
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // validate that the older expiration value is removed from ttl state
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 95000))
      )
    }
  }

  test("validate ttl removal keeps value in state - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        // advance clock and update state to remove ttl
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, InputEvent("k1", "put", 1, null)),
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // validate value is not expired
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // validate ttl value is removed in the value state column family
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // validate ttl state still has old ttl value present
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        // advance clock after older expiration value
        AdvanceManualClock(30 * 1000),
        // ensure unexpired value is still present in the state
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // validate that the older expiration value is removed from ttl state
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer()
      )
    }
  }

  test("validate state is evicted at ttl expiry for no data batch" +
    " - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
    classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(
          Trigger.ProcessingTime("1 second"),
          triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        Execute("beforeNoDataBatch") { q =>
          logWarning(s"before running a no data batch ${q.lastProgress.batchId}")
        },
        // advance clock so that state expires
        AdvanceManualClock(60 * 1000),
        // run a no data batch
        CheckNewAnswer(),
        Execute("noDataBatch") { q =>
          logWarning(s"running a no data batch ${q.lastProgress.batchId}")
        },
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // validate expired value is not returned
        CheckNewAnswer(),
        // ensure this state does not exist any longer in State
        AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer()
      )
    }
  }

  test("validate multiple value states - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val ttlKey = "k1"
      val noTtlKey = "k2"

      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          MultipleValueStatesTTLProcessor(ttlKey, noTtlKey),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent(ttlKey, "put", 1, Duration.ofMinutes(1))),
        AddData(inputStream, InputEvent(noTtlKey, "put", 2, Duration.ZERO)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get both state values, and make sure we get unexpired value
        AddData(inputStream, InputEvent(ttlKey, "get", -1, null)),
        AddData(inputStream, InputEvent(noTtlKey, "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent(ttlKey, 1, isTTLValue = false, -1),
          OutputEvent(noTtlKey, 2, isTTLValue = false, -1)
        ),
        // ensure ttl values were added correctly, and noTtlKey has no ttl values
        AddData(inputStream, InputEvent(ttlKey, "get_ttl_value_from_state", -1, null)),
        AddData(inputStream, InputEvent(noTtlKey, "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent(ttlKey, -1, isTTLValue = true, 61000)),
        AddData(inputStream, InputEvent(ttlKey, "get_values_in_ttl_state", -1, null)),
        AddData(inputStream, InputEvent(noTtlKey, "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent(ttlKey, -1, isTTLValue = true, 61000)),
        // advance clock after expiry
        AdvanceManualClock(60 * 1000),
        AddData(inputStream, InputEvent(ttlKey, "get", -1, null)),
        AddData(inputStream, InputEvent(noTtlKey, "get", -1, null)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        // validate ttlKey is expired, bot noTtlKey is still present
        CheckNewAnswer(OutputEvent(noTtlKey, 2, isTTLValue = false, -1)),
        // validate ttl value is removed in the value state column family
        AddData(inputStream, InputEvent(ttlKey, "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer()
      )
    }
  }

  test("validate state is evicted at ttl expiry - event time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { dir =>
        val inputStream = MemoryStream[InputEvent]
        val result = inputStream.toDS()
          .withWatermark("eventTime", "1 second")
          .groupByKey(x => x.key)
          .transformWithState(
            new ValueStateTTLProcessor(),
            TimeoutMode.NoTimeouts(),
            TTLMode.EventTimeTTL(),
            OutputMode.Append())

        val eventTime1 = Timestamp.valueOf("2024-01-01 00:00:00")
        val eventTime2 = Timestamp.valueOf("2024-01-01 00:02:00")
        val ttlExpiration = Timestamp.valueOf("2024-01-01 00:03:00")
        val ttlExpirationMs = ttlExpiration.getTime
        val eventTime3 = Timestamp.valueOf("2024-01-01 00:05:00")

        testStream(result)(
          StartStream(checkpointLocation = dir.getAbsolutePath),
          AddData(inputStream,
            InputEvent("k1", "put", 1, null, eventTime1, ttlExpiration)),
          CheckNewAnswer(),
          StopStream,
          StartStream(checkpointLocation = dir.getAbsolutePath),
          // get this state, and make sure we get unexpired value
          AddData(inputStream, InputEvent("k1", "get", -1, null, eventTime2)),
          ProcessAllAvailable(),
          CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
          // ensure ttl values were added correctly
          AddData(inputStream,
            InputEvent("k1", "get_ttl_value_from_state", -1, null, eventTime2)),
          CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime2)),
          ProcessAllAvailable(),
          CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
          StopStream,
          StartStream(checkpointLocation = dir.getAbsolutePath),
          // increment event time so that key k1 expires
          AddData(inputStream, InputEvent("k2", "put", 1, null, eventTime3)),
          CheckNewAnswer(),
          StopStream,
          StartStream(checkpointLocation = dir.getAbsolutePath),
          // validate that k1 has expired
          AddData(inputStream, InputEvent("k1", "get", -1, null, eventTime3)),
          CheckNewAnswer(),
          // ensure this state does not exist any longer in State
          AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null, eventTime3)),
          CheckNewAnswer(),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime3)),
          CheckNewAnswer()
        )
      }
    }
  }

  test("validate ttl update updates the expiration timestamp - event time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .withWatermark("eventTime", "1 second")
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.EventTimeTTL(),
          OutputMode.Append())

      val eventTime1 = Timestamp.valueOf("2024-01-01 00:00:00")
      val eventTime2 = Timestamp.valueOf("2024-01-01 00:02:00")
      val ttlExpiration = Timestamp.valueOf("2024-01-01 00:03:00")
      val ttlExpirationMs = ttlExpiration.getTime
      val eventTime3 = Timestamp.valueOf("2024-01-01 00:05:00")

      testStream(result)(
        AddData(inputStream,
          InputEvent("k1", "put", 1, null, eventTime1, ttlExpiration)),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", 1, null, eventTime2)),
        ProcessAllAvailable(),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly
        AddData(inputStream,
          InputEvent("k1", "get_ttl_value_from_state", -1, null, eventTime2)),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime2)),
        ProcessAllAvailable(),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
        // remove tll expiration for key k1, and move watermark past previous ttl value
        AddData(inputStream, InputEvent("k1", "put", 2, null, eventTime3)),
        CheckNewAnswer(),
        // validate that the key still exists
        AddData(inputStream, InputEvent("k1", "get", -1, null, eventTime3)),
        CheckNewAnswer(OutputEvent("k1", 2, isTTLValue = false, -1)),
        // ensure this ttl expiration time has been removed from state
        AddData(inputStream,
          InputEvent("k1", "get_ttl_value_from_state", -1, null, eventTime2)),
        CheckNewAnswer(),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime2)),
        ProcessAllAvailable()
      )
    }
  }

  test("validate state is evicted at ttl expiry for no data batch" +
    " - event time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .withWatermark("eventTime", "1 minutes")
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.EventTimeTTL(),
          OutputMode.Append())

      val eventTime1 = Timestamp.valueOf("2024-01-01 00:00:00")
      val eventTime2 = Timestamp.valueOf("2024-01-01 00:02:00")
      val ttlExpiration = Timestamp.valueOf("2024-01-01 00:03:00")
      val ttlExpirationMs = ttlExpiration.getTime
      val eventTime3 = Timestamp.valueOf("2024-01-01 00:05:00")

      testStream(result)(
        AddData(inputStream,
          InputEvent("k1", "put", 1, null, eventTime1, ttlExpiration)),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", -1, null, eventTime2)),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly, and move watermark for next
        // batch to eventTime3
        AddData(inputStream,
          InputEvent("k1", "get_ttl_value_from_state", -1, null, eventTime3),
          InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime3)),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs),
          OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
        // run a no data batch
        CheckNewAnswer(),
        // validate that k1 has expired
        AddData(inputStream, InputEvent("k1", "get", -1, null, eventTime3)),
        CheckNewAnswer(),
        // ensure this state does not exist any longer in State
        AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null, eventTime3)),
        CheckNewAnswer(),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime3)),
        CheckNewAnswer()
      )
    }
  }

  test("validate ttl removal keeps value in state - event time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .withWatermark("eventTime", "1 second")
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.EventTimeTTL(),
          OutputMode.Append())

      val eventTime1 = Timestamp.valueOf("2024-01-01 00:00:00")
      val eventTime2 = Timestamp.valueOf("2024-01-01 00:02:00")
      val ttlExpiration = Timestamp.valueOf("2024-01-01 00:03:00")
      val ttlExpirationMs = ttlExpiration.getTime
      val eventTime3 = Timestamp.valueOf("2024-01-01 00:05:00")

      testStream(result)(
        AddData(inputStream, InputEvent("k1", "put", 1, null, eventTime1, ttlExpiration)),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", 1, null, eventTime2)),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null, eventTime2)),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime2)),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
        // update state and remove ttl
        AddData(inputStream, InputEvent("k1", "put", 2, null, eventTime2)),
        AddData(inputStream, InputEvent("k1", "get", -1, null, eventTime2)),
        // validate value is not expired
        CheckNewAnswer(OutputEvent("k1", 2, isTTLValue = false, -1)),
        // validate ttl value is removed in the value state column family
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null, eventTime2)),
        CheckNewAnswer(),
        // validate ttl state still has old ttl value present
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null, eventTime3)),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, ttlExpirationMs)),
        // eventTime has been advanced to eventTim3 which is after older expiration value
        // ensure unexpired value is still present in the state
        AddData(inputStream, InputEvent("k1", "get", -1, null, eventTime3)),
        CheckNewAnswer(OutputEvent("k1", 2, isTTLValue = false, -1)),
        // validate that the older expiration value is removed from ttl state
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        CheckNewAnswer()
      )
    }
  }

  test("validate only expired keys are removed from the state") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        AddData(inputStream, InputEvent("k2", "put", 2, Duration.ofMinutes(2))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // advance clock so that key k1 is expired
        AdvanceManualClock(60 * 1000),
        AddData(inputStream, InputEvent("k1", "get", 1, null)),
        AddData(inputStream, InputEvent("k2", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // validate k1 is expired and k2 is not
        CheckNewAnswer(OutputEvent("k2", 2, isTTLValue = false, -1)),
        // validate k1 is deleted from state
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // validate k2 exists in state
        AddData(inputStream, InputEvent("k2", "get_ttl_value_from_state", -1, null)),
        AddData(inputStream, InputEvent("k2", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k2", -1, isTTLValue = true, 121000),
          OutputEvent("k2", -1, isTTLValue = true, 121000))
      )
    }
  }
}
