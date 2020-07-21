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

import com.google.common.collect.ImmutableList;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.CreateClickStreamSessions;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
/** Unit tests for {@link CreateClickStreamSessions}. */
public class CreateClickStreamSessionsTest {

  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static final ClickStreamEvent EVENT =
      ClickStreamEvent.builder()
          .setUid(1L)
          .setSessionId("1")
          .setAgent("A")
          .setLng(1D)
          .setLat(1D)
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("browse")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  private static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_0_MINS =
      TimestampedValue.of(EVENT, Instant.ofEpochMilli(TIME));

  private static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_3_MINS =
      TimestampedValue.of(
          EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(3)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(3)));

  private static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_10_MINS =
      TimestampedValue.of(
          EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(10)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(10)));

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSessionization() throws Exception {

    Duration windowDuration = Duration.standardMinutes(5);

    PCollection<Row> sessions =
        pipeline
            .apply(
                Create.timestamped(
                    CLICK_STREAM_EVENT_0_MINS,
                    CLICK_STREAM_EVENT_3_MINS,
                    CLICK_STREAM_EVENT_10_MINS))
            .apply(new CreateClickStreamSessions(windowDuration));

    Schema eventSchema = pipeline.getSchemaRegistry().getSchema(ClickStreamEvent.class);

    Schema keySchema = Schema.builder().addStringField("sessionId").build();
    Schema schema =
        Schema.builder()
            .addRowField("key", keySchema)
            .addArrayField(
                "values",
                FieldType.row(pipeline.getSchemaRegistry().getSchema(ClickStreamEvent.class)))
            .build();

    SerializableFunction<ClickStreamEvent, Row> toRow =
        pipeline.getSchemaRegistry().getToRowFunction(ClickStreamEvent.class);

    Row row1 =
        Row.withSchema(schema)
            .addValues(Row.withSchema(keySchema).addValues("1").build())
            .addIterable(
                ImmutableList.of(
                    Row.withSchema(eventSchema)
                        .addValues(toRow.apply(CLICK_STREAM_EVENT_0_MINS.getValue()).getValues())
                        .build(),
                    Row.withSchema(eventSchema)
                        .addValues(toRow.apply(CLICK_STREAM_EVENT_3_MINS.getValue()).getValues())
                        .build()))
            .build();

    Row row2 =
        Row.withSchema(schema)
            .addValues(Row.withSchema(keySchema).addValues("1").build())
            .addIterable(
                ImmutableList.of(
                    Row.withSchema(eventSchema)
                        .addValues(toRow.apply(CLICK_STREAM_EVENT_10_MINS.getValue()).getValues())
                        .build()))
            .build();

    PAssert.that(sessions).containsInAnyOrder(ImmutableList.of(row1, row2));

    pipeline.run();
  }
}
