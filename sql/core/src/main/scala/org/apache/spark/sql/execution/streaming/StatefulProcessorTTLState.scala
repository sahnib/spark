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
package org.apache.spark.sql.execution.streaming

import java.time.Duration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeTTL, NoTTL, ProcessingTimeTTL}
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.TTLMode
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, NullType, StructField, StructType}

class StatefulProcessorTTLState(
    ttlMode: TTLMode,
    stateName: String,
    store: StateStore,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkMs: Option[Long],
    createColumnFamily: Boolean = true) {

  private val ttlColumnFamilyName = s"_ttl_$stateName"

  private val schemaForKeyRow = new StructType()
    .add("expirationMs", LongType)
    .add("groupingKey", BinaryType)
    .add("userKey", BinaryType)
  private val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  private val ttlKeyEncoder = UnsafeProjection.create(schemaForKeyRow)

  // empty row used for values
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  validate()
  if (createColumnFamily) {
    store.createColFamilyIfAbsent(ttlColumnFamilyName, schemaForKeyRow, 0,
      schemaForValueRow, isInternal = true)
  }

  private def validate(): Unit = {
    ttlMode match {
      case NoTTL =>
        throw new RuntimeException()
      case ProcessingTimeTTL if batchTimestampMs.isEmpty =>
        throw new IllegalStateException()
      case EventTimeTTL if eventTimeWatermarkMs.isEmpty =>
        throw new IllegalStateException()
      case _ =>
    }
  }

  private def expiredKeysIterator(): Iterator[InternalRow] = {
    // TODO(sahnib): Need to merge Anish's changes
    store.iterator(ttlColumnFamilyName).foreach { kv =>

    }

    Iterator.empty
  }

  def upsertTTLForStateKey(
      expirationMs: Long,
      groupingKey: Array[Byte],
      userKey: Option[Array[Byte]]): Unit = {
    val encodedTtlKey = ttlKeyEncoder(InternalRow(expirationMs,
      groupingKey, userKey.orNull))
    store.put(encodedTtlKey, EMPTY_ROW, ttlColumnFamilyName)
  }
}

object StatefulProcessorTTLState {
  def apply(
      ttlMode: TTLMode,
      stateName: String,
      store: StateStore,
      batchTimestampMs: Option[Long],
      eventTimeWatermarkMs: Option[Long]): StatefulProcessorTTLState = {
    new StatefulProcessorTTLState(ttlMode, stateName, store, batchTimestampMs, eventTimeWatermarkMs)
  }

  def calculateExpirationTimeForDuration(
      ttlMode: TTLMode,
      ttlDuration: Duration,
      batchTimestampMs: Option[Long],
      eventTimeWatermarkMs: Option[Long]): Long = {
    if (ttlMode == TTLMode.ProcessingTimeTTL()) {
      batchTimestampMs.get + ttlDuration.toMillis
    } else if (ttlMode == TTLMode.EventTimeTTL()) {
      eventTimeWatermarkMs.get + ttlDuration.toMillis
    } else {
      -1L
    }
  }

  def getCurrentExpirationTime(
      ttlMode: TTLMode,
      batchTimestampMs: Option[Long],
      eventTimeWatermarkMs: Option[Long]): Long = {
    if (ttlMode == TTLMode.ProcessingTimeTTL()) {
      batchTimestampMs.get
    } else if (ttlMode == TTLMode.EventTimeTTL()) {
      eventTimeWatermarkMs.get
    } else {
      -1L
    }
  }

  def isExpired(
      ttlMode: TTLMode,
      expirationMs: Long,
      batchTimestampMs: Option[Long],
      eventTimeWatermarkMs: Option[Long]): Boolean = {
    if (ttlMode == TTLMode.ProcessingTimeTTL()) {
      batchTimestampMs.get > expirationMs
    } else if (ttlMode == TTLMode.EventTimeTTL()) {
      eventTimeWatermarkMs.get > expirationMs
    } else {
      false
    }
  }

  def encodedTtlModeValue(ttLMode: TTLMode): Short = {
    ttLMode match {
      case NoTTL =>
        0
      case ProcessingTimeTTL =>
        1
      case EventTimeTTL =>
        2
    }
  }

  def decodedTtlMode(encodedVal: Short): TTLMode = {
    encodedVal match {
      case 0 =>
        TTLMode.NoTTL()
      case 1 =>
        TTLMode.ProcessingTimeTTL()
      case 2 =>
        TTLMode.EventTimeTTL()
      case _ =>
        throw new IllegalStateException("encodedTtlValue should be <= 2")
    }
  }
}
