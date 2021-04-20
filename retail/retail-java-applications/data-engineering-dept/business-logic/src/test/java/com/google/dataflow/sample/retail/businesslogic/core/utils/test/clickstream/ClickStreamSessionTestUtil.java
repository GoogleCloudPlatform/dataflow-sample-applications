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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.dataflow.sample.retail.businesslogic.core.utils.test.clickstream;

import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class ClickStreamSessionTestUtil {

  public static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  public static final ClickStreamEvent EVENT =
      ClickStreamEvent.builder()
          .setUid(1L)
          .setClientId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("browse")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  public static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_0_MINS =
      TimestampedValue.of(ClickStreamSessionTestUtil.EVENT, Instant.ofEpochMilli(TIME));

  public static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_1_MINS =
      TimestampedValue.of(
          ClickStreamSessionTestUtil.EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(1)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(1)));

  public static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_2_MINS =
      TimestampedValue.of(
          ClickStreamSessionTestUtil.EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(2)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(2)));

  public static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_3_MINS =
      TimestampedValue.of(
          ClickStreamSessionTestUtil.EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(3)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(3)));

  public static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_4_MINS =
      TimestampedValue.of(
          ClickStreamSessionTestUtil.EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(4)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(4)));

  public static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_10_MINS =
      TimestampedValue.of(
          ClickStreamSessionTestUtil.EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(10)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(10)));
}
