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

import java.time.Duration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.{MemoryStream, ValueStateImplWithTTL}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock


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

    for (row <- inputRows) {
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

    for (row <- inputRows) {
      val resultIterator = TTLInputProcessFunction.processRow(_ttlMode, row, state)
      resultIterator.foreach { r =>
        results = r :: results
      }
    }
    results.iterator
  }
}
class ValueStateTTLSuite extends TransformWithStateTTLSuiteBase {

  import testImplicits._
  override def getProcessor(): StatefulProcessor[String, InputEvent, OutputEvent] = {
    new ValueStateTTLProcessor()
  }

  test("validate multiple value states - processing time ttl") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val ttlKey = "k1"
      val noTtlKey = "k2"

      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          MultipleValueStatesTTLProcessor(ttlKey, noTtlKey),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL())

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
}
