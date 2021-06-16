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

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.TestMax;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.TestMin;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.TestSum;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** TODO switch whole TSAccum building tests to validatePropertyTests */
public class GenerateComputationTestWithAccumToSeq {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  /* Simple test to check values are processed into TSAccum Correctly */
  public void testCreateTSAccumSingleKeySingleWindowDoubleType() {

    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(TSDataTestUtils.DOUBLE_POINT_1_A_A)
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS))
            .addElements(TSDataTestUtils.DOUBLE_POINT_2_A_A)
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_TEN_SECS))
            .addElements(TSDataTestUtils.DOUBLE_POINT_3_A_A)
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccum>> result =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(30))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(30))
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .setEnableGapFill(false)
                    .build());

    Instant start = Instant.ofEpochMilli(TSDataTestUtils.START);

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of(
                TSDataTestUtils.KEY_A_A,
                TSAccum.newBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A)
                    .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                    .setUpperWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START),
                            Durations.fromSeconds(30)))
                    .setHasAGapFillMessage(false)
                    .putDataStore(
                        Indicators.FIRST_TIMESTAMP.name(),
                        CommonUtils.createNumData(TSDataTestUtils.START))
                    .putDataStore(
                        Indicators.LAST_TIMESTAMP.name(),
                        CommonUtils.createNumData(TSDataTestUtils.PLUS_TEN_SECS))
                    .putDataStore(Indicators.MAX.name(), TSDataTestUtils.DATA_3.getData())
                    .putDataStore(Indicators.MIN.name(), TSDataTestUtils.DATA_1.getData())
                    .putDataStore(Indicators.SUM.name(), Data.newBuilder().setDoubleVal(6d).build())
                    .putDataStore(
                        Indicators.DATA_POINT_COUNT.name(), Data.newBuilder().setLongVal(3).build())
                    .putDataStore(
                        Indicators.FIRST.name(), TSDataTestUtils.DOUBLE_POINT_1_A_A.getData())
                    .putDataStore(
                        Indicators.LAST.name(), TSDataTestUtils.DOUBLE_POINT_3_A_A.getData())
                    .build()));
    p.run();
  }

  @Test
  /* Simple test to check Gap fill True value moves downstream */
  public void testCreateTSAccumSingleKeySingleWindowDoubleTypeWithGapFill() {

    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                TSDataTestUtils.DOUBLE_POINT_1_A_A.toBuilder().setIsAGapFillMessage(true).build())
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccum>> result =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(30))
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .setEnableGapFill(false)
                    .build())
            .apply(Filter.by(x -> !x.getValue().getHasAGapFillMessage()));

    PAssert.that(result).empty();

    p.run();
  }

  @Test
  /* Simple test to check values are processed from TSAccum -> TSAccumeSequence Correctly */
  public void testCreateTSAccumSequenceFromSingleAccum() {

    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(TSDataTestUtils.DOUBLE_POINT_1_A_A)
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccumSequence>> result =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .setEnableGapFill(false)
                    .build())
            .apply(
                ConvertAccumToSequence.builder()
                    .setWindow(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                    .build());

    Instant start = Instant.ofEpochMilli(TSDataTestUtils.START);

    TSAccum first =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .putDataStore(
                Indicators.FIRST_TIMESTAMP.name(), CommonUtils.createNumData(TSDataTestUtils.START))
            .putDataStore(
                Indicators.LAST_TIMESTAMP.name(), CommonUtils.createNumData(TSDataTestUtils.START))
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
            .setHasAGapFillMessage(false)
            .putDataStore(Indicators.MAX.name(), TSDataTestUtils.DATA_1.getData())
            .putDataStore(Indicators.MIN.name(), TSDataTestUtils.DATA_1.getData())
            .putDataStore(Indicators.SUM.name(), Data.newBuilder().setDoubleVal(1d).build())
            .putDataStore(
                Indicators.DATA_POINT_COUNT.name(), Data.newBuilder().setLongVal(1).build())
            .putDataStore(Indicators.FIRST.name(), TSDataTestUtils.DOUBLE_POINT_1_A_A.getData())
            .putDataStore(Indicators.LAST.name(), TSDataTestUtils.DOUBLE_POINT_1_A_A.getData())
            .build();

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of(
                TSDataTestUtils.KEY_A_A,
                TSAccumSequence.newBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A)
                    .addAccums(first)
                    .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                    .setUpperWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
                    .setDuration(Durations.fromSeconds(5))
                    .setCount(1)
                    .build()));
    p.run();
  }

  @Test
  /* Simple test to check values are processed from 3 TSAccums to TSAccumeSequence Correctly */
  public void testCreateTSAccumSequenceFromThreeAccumFixedWindow() {

    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(TSDataTestUtils.DOUBLE_POINT_1_A_A)
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS))
            .addElements(TSDataTestUtils.DOUBLE_POINT_2_A_A)
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_TEN_SECS))
            .addElements(TSDataTestUtils.DOUBLE_POINT_3_A_A)
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccumSequence>> result =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .setEnableGapFill(false)
                    .build())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(TSKey.class), TypeDescriptor.of(TSAccum.class)))
                    .<KV<TSKey, TSAccum>>via(
                        x -> KV.of(x.getKey(), x.getValue().toBuilder().clearDataStore().build())))
            .apply(
                ConvertAccumToSequence.builder()
                    .setWindow(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                    .build());
    TSAccum first =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
            .build();

    TSAccum second =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS),
                    Durations.fromSeconds(5)))
            .build();
    TSAccum third =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS), Durations.fromSeconds(5)))
            .build();

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of(
                TSDataTestUtils.KEY_A_A,
                TSAccumSequence.newBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A)
                    .addAccums(first)
                    .addAccums(second)
                    .addAccums(third)
                    .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                    .setUpperWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START),
                            Durations.fromSeconds(30)))
                    .setDuration(Durations.fromSeconds(30))
                    .setCount(3)
                    .build()));
    p.run();
  }

  @Test
  /* Simple test to check accum sequence is not created if TSAccum are invalid with missing metrics. */
  public void testCreateTSAccumSequenceFromThreeInvalidAccumFixedWindow() {

    KV<TSKey, TSAccum> first =
        KV.of(
            TSDataTestUtils.KEY_A_A,
            TSAccum.newBuilder()
                .setKey(TSDataTestUtils.KEY_A_A)
                .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                .setUpperWindowBoundary(
                    Timestamps.add(
                        Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
                .putDataStore("A", Data.newBuilder().build())
                .build());

    KV<TSKey, TSAccum> second =
        KV.of(
            TSDataTestUtils.KEY_A_A,
            TSAccum.newBuilder()
                .setKey(TSDataTestUtils.KEY_A_A)
                .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
                .setUpperWindowBoundary(
                    Timestamps.add(
                        Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS),
                        Durations.fromSeconds(5)))
                .putDataStore("A", Data.newBuilder().build())
                .build());
    KV<TSKey, TSAccum> third =
        KV.of(
            TSDataTestUtils.KEY_A_A,
            TSAccum.newBuilder()
                .setKey(TSDataTestUtils.KEY_A_A)
                .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS))
                .setUpperWindowBoundary(
                    Timestamps.add(
                        Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS),
                        Durations.fromSeconds(5)))
                .putDataStore("B", Data.newBuilder().build())
                .build());

    TestStream<KV<TSKey, TSAccum>> stream =
        TestStream.create(CommonUtils.getKvTSAccumCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(first)
            .addElements(second)
            .addElements(third)
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccumSequence>> result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
            .apply(
                ConvertAccumToSequence.builder()
                    .setWindow(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                    .build());
    PAssert.that(result).empty();
    p.run();
  }

  @Test
  /* Simple test to check values are processed from 3 TSAccums to TSAccumeSequence Correctly */
  public void testCreateTSAccumSequenceFromThreeAccumSlidingWindow() {

    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(TSDataTestUtils.DOUBLE_POINT_1_A_A)
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS))
            .addElements(TSDataTestUtils.DOUBLE_POINT_2_A_A)
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_TEN_SECS))
            .addElements(TSDataTestUtils.DOUBLE_POINT_3_A_A)
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccumSequence>> result =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .setEnableGapFill(false)
                    .build())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(TSKey.class), TypeDescriptor.of(TSAccum.class)))
                    .<KV<TSKey, TSAccum>>via(
                        x -> KV.of(x.getKey(), x.getValue().toBuilder().clearDataStore().build())))
            .apply(
                ConvertAccumToSequence.builder()
                    .setWindow(
                        Window.into(
                            SlidingWindows.of(Duration.standardSeconds(5))
                                .every(Duration.standardSeconds(5))))
                    .build());

    TSAccum first =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
            .build();

    TSAccum second =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS),
                    Durations.fromSeconds(5)))
            .build();
    TSAccum third =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS), Durations.fromSeconds(5)))
            .build();

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of(
                TSDataTestUtils.KEY_A_A,
                TSAccumSequence.newBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A)
                    .addAccums(first)
                    .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                    .setUpperWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
                    .setDuration(Durations.fromSeconds(5))
                    .setCount(1)
                    .build()),
            KV.of(
                TSDataTestUtils.KEY_A_A,
                TSAccumSequence.newBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A)
                    .addAccums(second)
                    .setLowerWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
                    .setUpperWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START),
                            Durations.fromSeconds(10)))
                    .setDuration(Durations.fromSeconds(5))
                    .setCount(1)
                    .build()),
            KV.of(
                TSDataTestUtils.KEY_A_A,
                TSAccumSequence.newBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A)
                    .addAccums(third)
                    .setLowerWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START),
                            Durations.fromSeconds(10)))
                    .setUpperWindowBoundary(
                        Timestamps.add(
                            Timestamps.fromMillis(TSDataTestUtils.START),
                            Durations.fromSeconds(15)))
                    .setDuration(Durations.fromSeconds(5))
                    .setCount(1)
                    .build()));
    p.run();
  }
}
