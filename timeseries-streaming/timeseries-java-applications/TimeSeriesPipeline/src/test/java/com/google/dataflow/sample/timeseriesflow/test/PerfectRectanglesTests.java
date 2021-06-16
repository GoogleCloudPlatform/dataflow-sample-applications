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
package com.google.dataflow.sample.timeseriesflow.test;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PerfectRectanglesTests {

  static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  static final TSDataPoint DATA_POINT_A_A =
      TSDataPoint.newBuilder()
          .setKey(TSKey.newBuilder().setMajorKey("A").setMinorKeyString("A").build())
          .setTimestamp(Timestamps.fromMillis(NOW.getMillis()))
          .setData(CommonUtils.createNumData(5))
          .build();

  static final TSDataPoint DATA_POINT_A_B =
      TSDataPoint.newBuilder()
          .setKey(TSKey.newBuilder().setMajorKey("A").setMinorKeyString("B").build())
          .setData(CommonUtils.createNumData(5))
          .setTimestamp(Timestamps.fromMillis(NOW.getMillis()))
          .build();

  public TestStream<KV<TSKey, TSDataPoint>> getStreamNoGapImperfectWatermark() {
    // Stream has gaps at mid point along the stream.
    // There should be continuous values in every window as well as a final Gap value when the
    // stream finally stops
    return TestStream.create(CommonUtils.getKvTSDataPointCoder())
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
        .addElements(
            TimestampedValue.of(
                KV.of(DATA_POINT_A_A.getKey(), DATA_POINT_A_A), new Instant(TSDataTestUtils.START)))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    DATA_POINT_A_A.getKey(),
                    DATA_POINT_A_A
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1000))
                        .build()),
                new Instant(TSDataTestUtils.START + 1000)))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    DATA_POINT_A_A.getKey(),
                    DATA_POINT_A_A
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2000))
                        .build()),
                new Instant(TSDataTestUtils.START + 2000)))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    DATA_POINT_A_A.getKey(),
                    DATA_POINT_A_A
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 3000))
                        .build()),
                new Instant(TSDataTestUtils.START + 3000)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 4000))
        .advanceWatermarkToInfinity();
  }

  public TestStream<KV<TSKey, TSDataPoint>> getStreamWithGap() {
    // Stream has gaps at mid point along the stream.
    // There should be continuous values in every window as well as a final Gap value when the
    // stream finally stops
    return TestStream.create(CommonUtils.getKvTSDataPointCoder())
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
        .addElements(
            TimestampedValue.of(
                KV.of(DATA_POINT_A_B.getKey(), DATA_POINT_A_B), new Instant(TSDataTestUtils.START)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 1000))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    DATA_POINT_A_B.getKey(),
                    DATA_POINT_A_B
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1000))
                        .build()),
                new Instant(TSDataTestUtils.START + 1000)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 2000))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    DATA_POINT_A_B.getKey(),
                    DATA_POINT_A_B
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 3000))
                        .build()),
                new Instant(TSDataTestUtils.START + 3000)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 4000))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    DATA_POINT_A_B.getKey(),
                    DATA_POINT_A_B
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                        .build()),
                new Instant(TSDataTestUtils.START + 4000)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 5000))
        .advanceWatermarkToInfinity();
  }

  public TestStream<KV<TSKey, TSDataPoint>> getStreamWithLargerThanTTLGap(TSDataPoint datapoint) {
    // Stream has gaps at mid point along the stream.
    // There should be continuous values in every window as well as a final Gap value when the
    // stream finally stops
    return TestStream.create(CommonUtils.getKvTSDataPointCoder())
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
        .addElements(
            TimestampedValue.of(
                KV.of(datapoint.getKey(), datapoint), new Instant(TSDataTestUtils.START)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 1000))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    datapoint.getKey(),
                    datapoint
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1000))
                        .build()),
                new Instant(TSDataTestUtils.START + 1000)))
        //        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 2000))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    datapoint.getKey(),
                    datapoint
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                        .build()),
                new Instant(TSDataTestUtils.START + 4000)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 5000))
        .addElements(
            TimestampedValue.of(
                KV.of(
                    datapoint.getKey(),
                    datapoint
                        .toBuilder()
                        .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                        .build()),
                new Instant(TSDataTestUtils.START + 6000)))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 7000))
        .advanceWatermarkToInfinity();
  }

  @Test
  public void testNoGaps() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamNoGapImperfectWatermark())
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(
                        Duration.standardSeconds(1), Duration.ZERO)
                    .enablePreviousValueFill())
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 1000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 3000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 3000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 4000), Duration.standardSeconds(1)))
        .empty();
    p.run();
  }

  @Test
  public void testGapsMidPoint() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamWithGap())
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill())
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setIsAGapFillMessage(true)
                .build());
    p.run();
  }

  @Test
  public void testGapsMidPointWithExcludeList() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamWithGap())
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill()
                    .withPreviousValueFillExcludeList(ImmutableList.of(DATA_POINT_A_B.getKey())))
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setData(CommonUtils.createNumData(0))
                .setIsAGapFillMessage(true)
                .build());
    p.run();
  }

  @Test
  public void testGapsMidPointWithExcludeListMinorKey() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamWithGap())
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill()
                    .withPreviousValueFillExcludeList(
                        ImmutableList.of(
                            DATA_POINT_A_B.getKey().toBuilder().clearMajorKey().build())))
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setData(CommonUtils.createNumData(0))
                .setIsAGapFillMessage(true)
                .build());
    p.run();
  }

  @Test
  public void testWindowAfterGap() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamWithGap())
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill())
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 1000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 3000 - 1))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 3000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 3000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 4000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 5000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000 - 1))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 6000), Duration.standardSeconds(1)))
        .empty();

    p.run();
  }

  @Test
  public void testLargerThanTTLGapsMidPoint() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamWithLargerThanTTLGap(DATA_POINT_A_A))
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill())
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 3000), Duration.standardSeconds(1)))
        .empty()
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 4000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 5000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 5999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 6000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 7000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 7999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 8000), Duration.standardSeconds(1)))
        .empty();

    p.run();
  }

  @Test
  public void testLargerThanTTLGapsMidPointTwoKeys() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        PCollectionList.of(p.apply(getStreamWithLargerThanTTLGap(DATA_POINT_A_A)))
            .and(p.apply(getStreamWithLargerThanTTLGap(DATA_POINT_A_B)))
            .apply(Flatten.pCollections())
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill())
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setIsAGapFillMessage(true)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 3000), Duration.standardSeconds(1)))
        .empty()
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 4000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                .setIsAGapFillMessage(false)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 5000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 5999))
                .setIsAGapFillMessage(true)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 5999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 6000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                .setIsAGapFillMessage(false)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 7000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 7999))
                .setIsAGapFillMessage(true)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 7999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 8000), Duration.standardSeconds(1)))
        .empty();

    p.run();
  }

  @Test
  public void testAbsoluteStopGapsMidPoint() {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamWithLargerThanTTLGap(DATA_POINT_A_B))
            .apply(
                PerfectRectangles.withWindowAndAbsoluteStop(
                        Duration.standardSeconds(1),
                        Instant.ofEpochMilli(TSDataTestUtils.START + 8000))
                    .enablePreviousValueFill())
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 3000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 3999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 4000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 5000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 5999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 6000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 7000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 7999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 8000), Duration.standardSeconds(1)))
        .empty();

    p.run();
  }

  @Test
  public void testWithinTTLSingleKeyFillWithZeroValue() {

    Instant now = Instant.parse("2000-01-01T00:00:00Z");
    Duration ttlDuration = Duration.standardSeconds(2);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of(DATA_POINT_A_B.getKey(), DATA_POINT_A_B), now)))
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(
                    Duration.standardSeconds(1), ttlDuration))
            .apply(Values.create());

    PAssert.that(perfectRectangle)
        .containsInAnyOrder(
            DATA_POINT_A_B,
            DATA_POINT_A_B
                .toBuilder()
                .setData(TSDataUtils.getZeroValueForType(DATA_POINT_A_B.getData()))
                .setIsAGapFillMessage(true)
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(2)).getMillis() - 1))
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setData(TSDataUtils.getZeroValueForType(DATA_POINT_A_B.getData()))
                .setIsAGapFillMessage(true)
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(3)).getMillis() - 1))
                .build());

    p.run();
  }

  @Test
  public void testWithinTTLSingleKeySingleFillWithLastKnownValue() {

    Instant now = Instant.parse("2000-01-01T00:00:00Z");
    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of(DATA_POINT_A_B.getKey(), DATA_POINT_A_B), now)))
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill())
            .apply(Values.create());

    PAssert.that(perfectRectangle)
        .containsInAnyOrder(
            DATA_POINT_A_B,
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(2)).getMillis() - 1))
                .setIsAGapFillMessage(true)
                .build());

    p.run();
  }

  @Test
  public void testWithinAbsoluteBound() {

    Instant now = Instant.parse("2000-01-01T00:00:00Z");

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of(DATA_POINT_A_B.getKey(), DATA_POINT_A_B), now)))
            .apply(
                PerfectRectangles.withWindowAndAbsoluteStop(
                        Duration.standardSeconds(1), Instant.parse("2000-01-01T00:00:03Z"))
                    .enablePreviousValueFill())
            .apply(Values.create());

    perfectRectangle.apply(new Print<>());

    PAssert.that(perfectRectangle)
        .containsInAnyOrder(
            DATA_POINT_A_B,
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(2)).getMillis() - 1))
                .setIsAGapFillMessage(true)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(3)).getMillis() - 1))
                .setIsAGapFillMessage(true)
                .build());

    p.run();
  }

  @Test
  public void testEarlySuppressionWithAbsoluteBound() {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(getStreamWithLargerThanTTLGap(DATA_POINT_A_B))
            .apply(
                PerfectRectangles.withWindowAndAbsoluteStop(
                        Duration.standardSeconds(1),
                        Instant.ofEpochMilli(TSDataTestUtils.START + 8000))
                    .enablePreviousValueFill()
                    .suppressEarlyValuesWithStartTime(
                        Instant.ofEpochMilli(TSDataTestUtils.START + 5000)))
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .empty()
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 3000), Duration.standardSeconds(1)))
        .empty()
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 4000), Duration.standardSeconds(1)))
        .empty()
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 5000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 5999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 6000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 7000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 7999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 8000), Duration.standardSeconds(1)))
        .empty();

    p.run();
  }

  @Test
  public void testGarbageCollection() {}

  public TestStream<KV<TSKey, TSDataPoint>> getStreamWithEventTimeMismatch(TSDataPoint datapoint) {
    // Stream has gaps at mid point along the stream.
    // There should be continuous values in every window as well as a final Gap value when the
    // stream finally stops
    // In this stream the value of the Event time will be earlier than the Source TimestampedValue
    return TestStream.create(CommonUtils.getKvTSDataPointCoder())
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
        .addElements(KV.of(datapoint.getKey(), datapoint))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 1000))
        .addElements(
            KV.of(
                datapoint.getKey(),
                datapoint
                    .toBuilder()
                    .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1000))
                    .build()))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 4000))
        .addElements(
            KV.of(
                datapoint.getKey(),
                datapoint
                    .toBuilder()
                    .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                    .build()))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 6000))
        .addElements(
            KV.of(
                datapoint.getKey(),
                datapoint
                    .toBuilder()
                    .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                    .build()))
        .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START + 7000))
        .advanceWatermarkToInfinity();
  }

  @Test
  /*
  This test is to catch issues where the Source and data points do not have EvenTime alignment.
  For example where PubSubIO does not have TimestampedID set
  */
  public void testBeamTimestampDataPointTimestampMismatch() {

    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    PCollection<TSDataPoint> perfectRectangle =
        PCollectionList.of(p.apply(getStreamWithEventTimeMismatch(DATA_POINT_A_A)))
            .and(p.apply(getStreamWithEventTimeMismatch(DATA_POINT_A_B)))
            .apply(Flatten.pCollections())
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill())
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PAssert.that(perfectRectangle)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setIsAGapFillMessage(true)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 3000), Duration.standardSeconds(1)))
        .empty()
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 4000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                .setIsAGapFillMessage(false)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 4000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 5000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 5999))
                .setIsAGapFillMessage(true)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 5999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 6000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                .setIsAGapFillMessage(false)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 6000))
                .setIsAGapFillMessage(false)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 7000), Duration.standardSeconds(1)))
        .containsInAnyOrder(
            DATA_POINT_A_A
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 7999))
                .setIsAGapFillMessage(true)
                .build(),
            DATA_POINT_A_B
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 7999))
                .setIsAGapFillMessage(true)
                .build())
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 8000), Duration.standardSeconds(1)))
        .empty();

    p.run();
  }
}
