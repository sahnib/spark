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

package org.apache.spark.sql.execution.streaming.state

import java.time.Duration
import java.util.UUID

import org.apache.spark.{SparkIllegalArgumentException, SparkUnsupportedOperationException}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, ListStateImplWithTTL, StatefulProcessorHandleImpl}
import org.apache.spark.sql.streaming.{ListState, TimeoutMode, TTLMode, ValueState}

/**
 * Class that adds unit tests for ListState types used in arbitrary stateful
 * operators such as transformWithState
 */
class ListStateSuite extends StateVariableSuiteBase {
  // overwrite useMultipleValuesPerKey in base suite to be true for list state
  override def useMultipleValuesPerKey: Boolean = true

  private def testMapStateWithNullUserKey()(runListOps: ListState[Long] => Unit): Unit = {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TTLMode.NoTTL(), TimeoutMode.NoTimeouts())

      val listState: ListState[Long] = handle.getListState[Long]("listState", Encoders.scalaLong)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      val e = intercept[SparkIllegalArgumentException] {
        runListOps(listState)
      }

      checkError(
        exception = e,
        errorClass = "ILLEGAL_STATE_STORE_VALUE.NULL_VALUE",
        sqlState = Some("42601"),
        parameters = Map("stateName" -> "listState")
      )
    }
  }

  Seq("appendList", "put").foreach { listImplFunc =>
    test(s"Test list operation($listImplFunc) with null") {
      testMapStateWithNullUserKey() { listState =>
        listImplFunc match {
          case "appendList" => listState.appendList(null)
          case "put" => listState.put(null)
        }
      }
    }
  }

  test("List state operations for single instance") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TTLMode.NoTTL(), TimeoutMode.NoTimeouts())

      val testState: ListState[Long] = handle.getListState[Long]("testState", Encoders.scalaLong)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      // simple put and get test
      testState.appendValue(123)
      assert(testState.get().toSeq === Seq(123))
      testState.clear()
      assert(!testState.exists())
      assert(testState.get().toSeq === Seq.empty[Long])

      // put list test
      testState.appendList(Array(123, 456))
      assert(testState.get().toSeq === Seq(123, 456))
      testState.appendValue(789)
      assert(testState.get().toSeq === Seq(123, 456, 789))

      testState.clear()
      assert(!testState.exists())
      assert(testState.get().toSeq === Seq.empty[Long])
    }
  }

  test("List state operations for multiple instance") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TTLMode.NoTTL(), TimeoutMode.NoTimeouts())

      val testState1: ListState[Long] = handle.getListState[Long]("testState1", Encoders.scalaLong)
      val testState2: ListState[Long] = handle.getListState[Long]("testState2", Encoders.scalaLong)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      // simple put and get test
      testState1.appendValue(123)
      testState2.appendValue(456)
      assert(testState1.get().toSeq === Seq(123))
      assert(testState2.get().toSeq === Seq(456))
      testState1.clear()
      assert(!testState1.exists())
      assert(testState2.exists())
      assert(testState1.get().toSeq === Seq.empty[Long])

      // put list test
      testState1.appendList(Array(123, 456))
      assert(testState1.get().toSeq === Seq(123, 456))
      testState2.appendList(Array(123))
      assert(testState2.get().toSeq === Seq(456, 123))

      testState1.appendValue(789)
      assert(testState1.get().toSeq === Seq(123, 456, 789))
      assert(testState2.get().toSeq === Seq(456, 123))

      testState2.clear()
      assert(!testState2.exists())
      assert(testState1.exists())
      assert(testState2.get().toSeq === Seq.empty[Long])
    }
  }

  test("List state operations with list, value, another list instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TTLMode.NoTTL(), TimeoutMode.NoTimeouts())

      val listState1: ListState[Long] = handle.getListState[Long]("listState1", Encoders.scalaLong)
      val listState2: ListState[Long] = handle.getListState[Long]("listState2", Encoders.scalaLong)
      val valueState: ValueState[Long] = handle.getValueState[Long](
        "valueState", Encoders.scalaLong)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // simple put and get test
      valueState.update(123)
      listState1.appendValue(123)
      listState2.appendValue(456)
      assert(listState1.get().toSeq === Seq(123))
      assert(listState2.get().toSeq === Seq(456))
      assert(valueState.get() === 123)

      listState1.clear()
      valueState.clear()
      assert(!listState1.exists())
      assert(listState2.exists())
      assert(!valueState.exists())
      assert(listState1.get().toSeq === Seq.empty[Long])
    }
  }

  test("test TTL duration throws error for event time") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val eventTimeWatermarkMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TTLMode.EventTimeTTL(), TimeoutMode.NoTimeouts(),
        eventTimeWatermarkMs = Some(eventTimeWatermarkMs))

      val testState: ListStateImplWithTTL[String] = handle.getListState[String]("testState",
        Encoders.STRING).asInstanceOf[ListStateImplWithTTL[String]]
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      val ex1 = intercept[SparkUnsupportedOperationException] {
        testState.put(Array("v1"), Duration.ofMinutes(1))
      }

      checkError(
        ex1,
        errorClass = "STATEFUL_PROCESSOR_CANNOT_USE_TTL_DURATION_IN_EVENT_TIME_TTL_MODE",
        parameters = Map(
          "operationType" -> "put",
          "stateName" -> "testState"
        ),
        matchPVals = true
      )

      testState.put(Array("v1"), 10000)
      val ex2 = intercept[SparkUnsupportedOperationException] {
        testState.appendList(Array("v1"), Duration.ofMinutes(1))
      }

      checkError(
        ex2,
        errorClass = "STATEFUL_PROCESSOR_CANNOT_USE_TTL_DURATION_IN_EVENT_TIME_TTL_MODE",
        parameters = Map(
          "operationType" -> "appendList",
          "stateName" -> "testState"
        ),
        matchPVals = true
      )

      val ex3 = intercept[SparkUnsupportedOperationException] {
        testState.appendValue("v1", Duration.ofMinutes(1))
      }

      checkError(
        ex3,
        errorClass = "STATEFUL_PROCESSOR_CANNOT_USE_TTL_DURATION_IN_EVENT_TIME_TTL_MODE",
        parameters = Map(
          "operationType" -> "appendValue",
          "stateName" -> "testState"
        ),
        matchPVals = true
      )
    }
  }

  test("test negative TTL duration throws error") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val batchTimestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TTLMode.ProcessingTimeTTL(), TimeoutMode.NoTimeouts(),
        batchTimestampMs = Some(batchTimestampMs))

      val testState: ListStateImplWithTTL[String] = handle.getListState[String]("testState",
        Encoders.STRING).asInstanceOf[ListStateImplWithTTL[String]]
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      val ex1 = intercept[SparkUnsupportedOperationException] {
        testState.put(Array("v1"), Duration.ofMinutes(-1))
      }

      checkError(
        ex1,
        errorClass = "STATEFUL_PROCESSOR_TTL_VALUE_CANNOT_BE_NEGATIVE",
        parameters = Map(
          "operationType" -> "put",
          "stateName" -> "testState"
        ),
        matchPVals = true
      )
      testState.put(Array("v1"), Duration.ofMinutes(1))
      // test negative duration for appendList and appendValue
      val ex2 = intercept[SparkUnsupportedOperationException] {
        testState.appendList(Array("v1"), Duration.ofMinutes(-1))
      }

      checkError(
        ex2,
        errorClass = "STATEFUL_PROCESSOR_TTL_VALUE_CANNOT_BE_NEGATIVE",
        parameters = Map(
          "operationType" -> "appendList",
          "stateName" -> "testState"
        ),
        matchPVals = true
      )

      val ex3 = intercept[SparkUnsupportedOperationException] {
          testState.appendValue("v1", Duration.ofMinutes(-1))
      }

      checkError(
          ex3,
          errorClass = "STATEFUL_PROCESSOR_TTL_VALUE_CANNOT_BE_NEGATIVE",
          parameters = Map(
          "operationType" -> "appendValue",
          "stateName" -> "testState"
          ),
          matchPVals = true
      )
    }
  }

  test("test negative expirationMs throws error") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val eventTimeWatermarkMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TTLMode.EventTimeTTL(), TimeoutMode.NoTimeouts(),
        eventTimeWatermarkMs = Some(eventTimeWatermarkMs))

      val testState: ListStateImplWithTTL[String] = handle.getListState[String]("testState",
        Encoders.STRING).asInstanceOf[ListStateImplWithTTL[String]]
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      val ex1 = intercept[SparkUnsupportedOperationException] {
        testState.put(Array("v1"), -10)
      }

      checkError(
        ex1,
        errorClass = "STATEFUL_PROCESSOR_TTL_VALUE_CANNOT_BE_NEGATIVE",
        parameters = Map(
          "operationType" -> "put",
          "stateName" -> "testState"
        ),
        matchPVals = true
      )
      testState.put(Array("v1"), 10)

      val ex2 = intercept[SparkUnsupportedOperationException] {
          testState.appendList(Array("v1"), -10)
      }

      checkError(
          ex2,
          errorClass = "STATEFUL_PROCESSOR_TTL_VALUE_CANNOT_BE_NEGATIVE",
          parameters = Map(
          "operationType" -> "appendList",
          "stateName" -> "testState"
          ),
          matchPVals = true
      )

      val ex3 = intercept[SparkUnsupportedOperationException] {
          testState.appendValue("v1", -10)
      }

      checkError(
          ex3,
          errorClass = "STATEFUL_PROCESSOR_TTL_VALUE_CANNOT_BE_NEGATIVE",
          parameters = Map(
          "operationType" -> "appendValue",
          "stateName" -> "testState"
          ),
          matchPVals = true
      )
    }
  }
}
