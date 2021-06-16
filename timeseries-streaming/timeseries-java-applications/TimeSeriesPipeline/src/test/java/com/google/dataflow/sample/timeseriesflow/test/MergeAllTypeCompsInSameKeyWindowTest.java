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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSBaseCombiner;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.MergeAllTypeCompsInSameKeyWindow;
import com.google.protobuf.util.Timestamps;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** TODO switch whole TSAccum building tests to validatePropertyTests */
public class MergeAllTypeCompsInSameKeyWindowTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreateTSAccumMerge() {

    TestStream<KV<TSKey, TSAccum>> stream1 =
        TestStream.create(CommonUtils.getKvTSAccumCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                KV.of(
                    TSDataTestUtils.KEY_A_A,
                    TSAccum.newBuilder()
                        .setKey(TSDataTestUtils.KEY_A_A)
                        .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                        .setUpperWindowBoundary(
                            Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
                        .setHasAGapFillMessage(true)
                        .putDataStore(
                            Indicators.DATA_POINT_COUNT.name(),
                            Data.newBuilder().setLongVal(1).build())
                        .putDataStore("METRIC_A", Data.newBuilder().setIntVal(1).build())
                        .putDataStore("METRIC_B", Data.newBuilder().setIntVal(1).build())
                        .putMetadata(TSBaseCombiner._BASE_COMBINER, "t")
                        .build()))
            .advanceWatermarkToInfinity();

    TestStream<KV<TSKey, TSAccum>> stream2 =
        TestStream.create(CommonUtils.getKvTSAccumCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                KV.of(
                    TSDataTestUtils.KEY_A_A,
                    TSAccum.newBuilder()
                        .setKey(TSDataTestUtils.KEY_A_A)
                        .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                        .setUpperWindowBoundary(
                            Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
                        .setHasAGapFillMessage(false)
                        .putDataStore(
                            Indicators.DATA_POINT_COUNT.name(),
                            Data.newBuilder().setLongVal(1).build())
                        .putDataStore("METRIC_B", Data.newBuilder().setIntVal(1).build())
                        .putDataStore("METRIC_C", Data.newBuilder().setIntVal(1).build())
                        .putMetadata(TSBaseCombiner._BASE_COMBINER, "t")
                        .build()))
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccum>> allAccum =
        PCollectionList.of(
                p.apply("A", stream1)
                    .apply(
                        "WINDOW_T1",
                        Window.<KV<TSKey, TSAccum>>into(
                            FixedWindows.of(Duration.standardSeconds(10)))))
            .and(
                p.apply("B", stream2)
                    .apply(
                        "WINDOW_T2",
                        Window.<KV<TSKey, TSAccum>>into(
                            FixedWindows.of(Duration.standardSeconds(10)))))
            .apply(Flatten.pCollections())
            .apply(MergeAllTypeCompsInSameKeyWindow.create());

    // Strip out accums as they are unordered and test metadata
    PAssert.that(
            allAccum.apply(
                MapElements.into(TypeDescriptor.of(TSAccum.class))
                    .via(x -> x.getValue().toBuilder().clearDataStore().clearMetadata().build())))
        .containsInAnyOrder(
            TSAccum.newBuilder()
                .setKey(TSDataTestUtils.KEY_A_A)
                .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                .setUpperWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
                .setHasAGapFillMessage(true)
                .build());

    // Strip out accums as they are unordered and test metadata
    PAssert.that(
            allAccum.apply(
                FlatMapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .<KV<TSKey, TSAccum>>via(
                        x ->
                            (x.getValue().getDataStoreMap().entrySet().stream()
                                .map(
                                    k -> {
                                      if (k.getValue().getIntVal() != 0) {
                                        return KV.of(k.getKey(), k.getValue().getIntVal());
                                      }
                                      return KV.of(k.getKey(), (int) k.getValue().getLongVal());
                                    })
                                .collect(Collectors.toList())))))
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of(Indicators.DATA_POINT_COUNT.name(), 1),
                KV.of("METRIC_A", 1),
                KV.of("METRIC_B", 1),
                KV.of("METRIC_C", 1)));

    p.run();
  }

  // TODO catch actual exception
  @Test(expected = PipelineExecutionException.class)
  /* Simple test to check RSI Technical is created correctly */
  public void testFailureOnMultipleValuesForMetric() {

    // Key A-A will increase, Key A-B will decrease, Key A-C will remain stationary
    TestStream<KV<TSKey, TSAccum>> stream1 =
        TestStream.create(CommonUtils.getKvTSAccumCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                KV.of(
                    TSDataTestUtils.KEY_A_A,
                    TSAccum.newBuilder()
                        .setKey(TSDataTestUtils.KEY_A_A)
                        .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                        .setUpperWindowBoundary(
                            Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
                        .putDataStore("METRIC_A", Data.newBuilder().setIntVal(1).build())
                        .putDataStore("METRIC_B", Data.newBuilder().setIntVal(1).build())
                        .build()))
            .advanceWatermarkToInfinity();

    TestStream<KV<TSKey, TSAccum>> stream2 =
        TestStream.create(CommonUtils.getKvTSAccumCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                KV.of(
                    TSDataTestUtils.KEY_A_A,
                    TSAccum.newBuilder()
                        .setKey(TSDataTestUtils.KEY_A_A)
                        .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                        .setUpperWindowBoundary(
                            Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
                        .putDataStore("METRIC_B", Data.newBuilder().setIntVal(2).build())
                        .putDataStore("METRIC_C", Data.newBuilder().setIntVal(1).build())
                        .build()))
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccum>> allAccum =
        PCollectionList.of(
                p.apply("A", stream1)
                    .apply(
                        "WINDOW_T1",
                        Window.<KV<TSKey, TSAccum>>into(
                            FixedWindows.of(Duration.standardSeconds(10)))))
            .and(
                p.apply("B", stream2)
                    .apply(
                        "WINDOW_T2",
                        Window.<KV<TSKey, TSAccum>>into(
                            FixedWindows.of(Duration.standardSeconds(10)))))
            .apply(Flatten.pCollections())
            .apply(MergeAllTypeCompsInSameKeyWindow.create());

    // Strip out accums as they are unordered and test metadata
    PAssert.that(
            allAccum.apply(
                MapElements.into(TypeDescriptor.of(TSAccum.class))
                    .via(x -> x.getValue().toBuilder().clearDataStore().build())))
        .containsInAnyOrder(
            TSAccum.newBuilder()
                .setKey(TSDataTestUtils.KEY_A_A)
                .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
                .setUpperWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
                .build());

    p.run();
  }
}
