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
import com.google.dataflow.sample.retail.businesslogic.core.transforms.DeadLetterSink.SinkType;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.ValidateAndCorrectCSEvt;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.Item;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ValidateAndCorrectCSEvt}. TODO Remove Convert to / from left over code. */
@RunWith(JUnit4.class)
public class ValidateAndCorrectClickStreamEventsTests {
  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static final TimestampedValue<ClickStreamEvent> CLEAN_DATA =
      TimestampedValue.of(
          ClickStreamEvent.builder()
              .setEventTime("2000-01-01 00:00:00")
              .setUid(1L)
              .setClientId("1")
              .setAgent("A")
              .setPageRef("pageRef")
              .setPageTarget("pageTarget")
              .setEvent("event")
              .build(),
          Instant.ofEpochMilli(TIME));

  private static final ClickStreamEvent BAD_DATE_FORMAT =
      ClickStreamEvent.builder()
          .setEventTime("2000-XX-01 00:00:00")
          .setClientId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .build();

  private static final ClickStreamEvent DATE_IN_FUTURE =
      ClickStreamEvent.builder()
          .setEventTime("2000-01-02 00:00:00")
          .setUid(1L)
          .setClientId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .build();

  private static final ClickStreamEvent MISSING_ITEM_INFO =
      ClickStreamEvent.builder()
          .setEventTime("2000-01-01 00:00:00")
          .setClientId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("add_to_cart")
          .setItems(ImmutableList.of(Item.builder().setPrice("1").setItemId("1").build()))
          .build();

  private static final ClickStreamEvent CLEAN_DATE_TIMESTAMP =
      ClickStreamEvent.builder()
          .setEventTime("2000-XX-01 00:00:00")
          .setUid(1L)
          .setClientId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .build();

  private static final ClickStreamEvent MISSING_ITEM_INFO_BAD_DATE =
      ClickStreamEvent.builder()
          .setEventTime("2000-0X-01 00:00:00")
          .setClientId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("add_to_cart")
          .setItems(ImmutableList.of(Item.builder().setPrice("1").setItemId("1").build()))
          .build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCleanValidation() {

    PCollection<ClickStreamEvent> result =
        pipeline
            .apply(Create.timestamped(CLEAN_DATA))
            .apply("From", Convert.toRows())
            .apply(new ValidateAndCorrectCSEvt(SinkType.LOG))
            .apply(Convert.fromRows(ClickStreamEvent.class));

    PAssert.that(result)
        .containsInAnyOrder(CLEAN_DATA.getValue().toBuilder().setTimestamp(TIME).build());
    pipeline.run();
  }

  @Test
  public void testBadDateFormat() throws NoSuchSchemaException {

    SchemaCoder<ClickStreamEvent> schema =
        pipeline.getSchemaRegistry().getSchemaCoder(ClickStreamEvent.class);

    TestStream<ClickStreamEvent> ts =
        TestStream.create(schema)
            .advanceWatermarkTo(Instant.ofEpochMilli(TIME))
            .addElements(BAD_DATE_FORMAT)
            .advanceWatermarkToInfinity();

    PCollection<ClickStreamEvent> result =
        pipeline
            .apply(ts)
            .apply("From", Convert.toRows())
            .apply(new ValidateAndCorrectCSEvt(SinkType.LOG))
            .apply(Convert.fromRows(ClickStreamEvent.class));

    PAssert.that(result)
        .containsInAnyOrder(CLEAN_DATE_TIMESTAMP.toBuilder().setTimestamp(TIME).build());
    pipeline.run();
  }

  @Test
  public void testDateInFuture() throws NoSuchSchemaException {

    SchemaCoder<ClickStreamEvent> schema =
        pipeline.getSchemaRegistry().getSchemaCoder(ClickStreamEvent.class);

    TestStream<ClickStreamEvent> ts =
        TestStream.create(schema)
            .advanceWatermarkTo(Instant.ofEpochMilli(TIME))
            .addElements(DATE_IN_FUTURE)
            .advanceWatermarkToInfinity();

    PCollection<ClickStreamEvent> result =
        pipeline
            .apply(ts)
            .apply("From", Convert.toRows())
            .apply(new ValidateAndCorrectCSEvt(SinkType.LOG))
            .apply(Convert.fromRows(ClickStreamEvent.class));

    PAssert.that(result).containsInAnyOrder(DATE_IN_FUTURE.toBuilder().setTimestamp(TIME).build());
    pipeline.run();
  }

  @Test
  public void testMissingItems() throws NoSuchSchemaException {

    SchemaCoder<ClickStreamEvent> schema =
        pipeline.getSchemaRegistry().getSchemaCoder(ClickStreamEvent.class);

    TestStream<ClickStreamEvent> ts =
        TestStream.create(schema)
            .advanceWatermarkTo(Instant.ofEpochMilli(TIME))
            .addElements(MISSING_ITEM_INFO)
            .advanceWatermarkToInfinity();

    PCollection<ClickStreamEvent> result =
        pipeline
            .apply(ts)
            .apply("From", Convert.toRows())
            .apply(new ValidateAndCorrectCSEvt(SinkType.LOG))
            .apply(Convert.fromRows(ClickStreamEvent.class));

    PAssert.that(result)
        .containsInAnyOrder(
            MISSING_ITEM_INFO
                .toBuilder()
                .setTimestamp(TIME)
                .setItems(
                    ImmutableList.of(
                        Item.builder()
                            .setPrice("1")
                            .setItemId("1")
                            .setItemName("foo_name")
                            .setItemBrand("item_brand")
                            .setItemCat01("foo_category")
                            .build()))
                .build());
    pipeline.run();
  }

  @Test
  public void testBadDateFormatAndMissingItems() throws NoSuchSchemaException {

    SchemaCoder<ClickStreamEvent> schema =
        pipeline.getSchemaRegistry().getSchemaCoder(ClickStreamEvent.class);

    TestStream<ClickStreamEvent> ts =
        TestStream.create(schema)
            .advanceWatermarkTo(Instant.ofEpochMilli(TIME))
            .addElements(MISSING_ITEM_INFO_BAD_DATE)
            .advanceWatermarkToInfinity();

    PCollection<ClickStreamEvent> result =
        pipeline
            .apply(ts)
            .apply("From", Convert.toRows())
            .apply(new ValidateAndCorrectCSEvt(SinkType.LOG))
            .apply(Convert.fromRows(ClickStreamEvent.class));

    PAssert.that(result)
        .containsInAnyOrder(
            MISSING_ITEM_INFO_BAD_DATE
                .toBuilder()
                .setItems(
                    ImmutableList.of(
                        Item.builder()
                            .setPrice("1")
                            .setItemId("1")
                            .setItemName("foo_name")
                            .setItemBrand("item_brand")
                            .setItemCat01("foo_category")
                            .build()))
                .setTimestamp(TIME)
                .build());
    pipeline.run();
  }
}
