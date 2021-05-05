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
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.*;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.*;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** TODO switch whole TSAccum building tests to validatePropertyTests */
public class GenerateComputationTest {

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
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .setEnableGapFill(false)
                    .setType1FixedWindow(Duration.standardSeconds(30))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(30))
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
  /* Simple test to check all data types are processed into TSAccum Correctly */
  public void testCreateTSAccumFromAllDataTypes() {

    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                TSDataTestUtils.DOUBLE_POINT_1_A_A
                    .toBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString("double"))
                    .build())
            .addElements(
                TSDataTestUtils.DOUBLE_POINT_1_A_A
                    .toBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString("long"))
                    .setData(CommonUtils.createNumData(1L))
                    .setData(CommonUtils.createNumData(1L))
                    .build())
            .addElements(
                TSDataTestUtils.DOUBLE_POINT_1_A_A
                    .toBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString("integer"))
                    .setData(CommonUtils.createNumData(1L))
                    .setData(CommonUtils.createNumData(1))
                    .build())
            .addElements(
                TSDataTestUtils.DOUBLE_POINT_1_A_A
                    .toBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString("float"))
                    .setData(CommonUtils.createNumData(1L))
                    .setData(CommonUtils.createNumData(1.0F))
                    .build())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS))
            .advanceWatermarkToInfinity();

    PCollection<String> result =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(5))
                    .setEnableGapFill(false)
                    .build())
            .apply(
                WithKeys.<String, KV<TSKey, TSAccum>>of(
                    x -> x.getValue().getDataStoreMap().get("SUM").getDataPointCase().name()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), CommonUtils.getKvTSAccumCoder()))
            .apply(Keys.create())
            .apply(Distinct.create());

    PAssert.that(result).containsInAnyOrder("FLOAT_VAL", "LONG_VAL", "DOUBLE_VAL", "INT_VAL");
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

  @Test
  /* Simple test to check values are processed from 3 TSAccums to TSAccumeSequence Correctly */
  public void testCreateTSAccumSequenceFromThreeAccumSlidingWindowViaOptions() {

    TSFlowOptions options = p.getOptions().as(TSFlowOptions.class);
    options.setTypeOneComputationsLengthInSecs(5);
    // TODO Remove when just Type 1 is supported.
    options.setTypeTwoComputationsLengthInSecs(5);
    options.setOutputTimestepLengthInSecs(5);
    options.setGapFillEnabled(false);

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
                GenerateComputations.fromPiplineOptions(options)
                    .setBasicType1Metrics(
                        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class))
                    .build())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(TSKey.class), TypeDescriptor.of(TSAccum.class)))
                    .<KV<TSKey, TSAccum>>via(
                        x -> KV.of(x.getKey(), x.getValue().toBuilder().clearDataStore().build())))
            .apply(ConvertAccumToSequence.builder().build());

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

  @Test
  /* Test that multiple calls to Generate does not have name space collision */
  public void testNameSpaceCollision() {

    TimestampedValue<TSDataPoint> data =
        TimestampedValue.of(
            TSDataTestUtils.DOUBLE_POINT_1_A_A, Instant.ofEpochMilli(TSDataTestUtils.START));

    GenerateComputations generateComputations =
        GenerateComputations.builder()
            .setType1FixedWindow(Duration.standardSeconds(5))
            .setType2SlidingWindowDuration(Duration.standardSeconds(5))
            .setBasicType1Metrics(
                ImmutableList.of(TestSum.class, TestSum.class, TestMin.class, TestMax.class))
            .setBasicType2Metrics(ImmutableList.of(Test1Fn.class, Test2Fn.class))
            .setComplexType2Metrics(ImmutableList.of(TestComplex1GFn.class, TestComplex2GFn.class))
            .setEnableGapFill(false)
            .build();

    p.apply("Create_T1", Create.timestamped(data)).apply("T1", generateComputations);
    p.apply("Create_T2", Create.timestamped(data)).apply("T2", generateComputations);

    p.run();
  }

  /* Test ability to pass metrics as options  */
  @Test
  public void testMetricsFromOptions() {

    TimestampedValue<TSDataPoint> data =
        TimestampedValue.of(
            TSDataTestUtils.DOUBLE_POINT_1_A_A, Instant.ofEpochMilli(TSDataTestUtils.START));

    TSFlowOptions options = p.getOptions().as(TSFlowOptions.class);

    options.setTypeOneComputationsLengthInSecs(5);
    options.setTypeTwoComputationsLengthInSecs(5);
    options.setGapFillEnabled(false);

    options.setTypeOneBasicMetrics(
        ImmutableList.of("typeone.TestSum", "typeone.TestMin", "typeone.TestMax"));
    options.setTypeTwoBasicMetrics(ImmutableList.of("typetwo.Test2Fn", "typetwo.Test1Fn"));
    options.setTypeTwoComplexMetrics(
        ImmutableList.of("typetwo.TestComplex2GFn", "typetwo.TestComplex1GFn"));

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(p.getOptions().as(TSFlowOptions.class)).build();

    p.apply("Create_T1", Create.timestamped(data)).apply("T1", generateComputations);

    Assert.assertEquals(
        generateComputations.getBasicType1Metrics(),
        ImmutableList.of(TestSum.class, TestMin.class, TestMax.class));

    Assert.assertEquals(
        generateComputations.getBasicType2Metrics(),
        ImmutableList.of(Test2Fn.class, Test1Fn.class));

    Assert.assertEquals(
        generateComputations.getComplexType2Metrics(),
        ImmutableList.of(TestComplex2GFn.class, TestComplex1GFn.class));

    p.run();
  }
}
