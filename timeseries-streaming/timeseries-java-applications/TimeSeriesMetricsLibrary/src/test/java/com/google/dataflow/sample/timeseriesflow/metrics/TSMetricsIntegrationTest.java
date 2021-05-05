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
package com.google.dataflow.sample.timeseriesflow.metrics;

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.rsi.RSIGFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Max;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Min;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Sum;
import common.TSTestDataBaseline;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class TSMetricsIntegrationTest {

  public static void main(String[] args) {

    TSMetricsIntegrationTest.testCreateRSIDataflowService();
  }

  /* Simple test to check RSI Technical is created correctly */
  public static void testCreateRSIDataflowService() {
    // TODO add as automated test once integration framework is done.
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_1_A_A,
                        Instant.ofEpochMilli(TSTestDataBaseline.START)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_2_A_B
                            .toBuilder()
                            .setData(CommonUtils.createNumData(0D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.START)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_3_A_C
                            .toBuilder()
                            .setData(CommonUtils.createNumData(7D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.START)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_2_A_A
                            .toBuilder()
                            .setData(CommonUtils.createNumData(4D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.PLUS_FIVE_SECS)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_2_A_B
                            .toBuilder()
                            .setData(CommonUtils.createNumData(12D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.PLUS_FIVE_SECS)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_2_A_C
                            .toBuilder()
                            .setData(CommonUtils.createNumData(4D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.PLUS_FIVE_SECS)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_3_A_A
                            .toBuilder()
                            .setData(CommonUtils.createNumData(7D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.PLUS_TEN_SECS)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_2_A_B
                            .toBuilder()
                            .setData(CommonUtils.createNumData(6D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.PLUS_TEN_SECS)),
                    TimestampedValue.of(
                        TSTestDataBaseline.DOUBLE_POINT_1_A_C
                            .toBuilder()
                            .setData(CommonUtils.createNumData(1D))
                            .build(),
                        Instant.ofEpochMilli(TSTestDataBaseline.PLUS_TEN_SECS))))
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
                    .setComplexType2Metrics(ImmutableList.of(RSIGFn.class))
                    .build());

    // The sliding window will create partial values, to keep testing simple we just test
    // correctness of RSI for the full value

    PCollection<KV<TSKey, TSAccum>> fullAccum =
        techAccum.apply(
            Filter.by(
                x ->
                    x.getValue()
                            .getDataStoreOrThrow(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name())
                            .getIntVal()
                        == 3));

    PCollection<KV<TSKey, Double>> rs =
        fullAccum.apply(
            "RS",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            x.getValue()
                                .getDataStoreOrThrow(
                                    FsiTechnicalIndicators.RELATIVE_STRENGTH.name())
                                .getDoubleVal())));

    PCollection<KV<TSKey, Double>> rsi =
        fullAccum.apply(
            "RSI",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            Math.floor(
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.RELATIVE_STRENGTH_INDICATOR.name())
                                    .getDoubleVal()))));

    /*
    RS AvgG / AvgL
    RS Key_A_A = AvgGain 3 AvgLoss 0 =  Rule all gain -> 100
    RS Key_A_B = AvgGain 12 AvgLoss 6 = 12 / 6 = 2
    RS Key_A_C = AvgGain 0 AvgLoss 1 = 0 Rule all loss -> 0

    RSI 100 - (100 / (1 + rs));
    Key_A_A = 100 = 100 - ( 100 / 101) = 99
    Key_A_B = 2 = 100 - (100 / (1+2)) = 100 - 33.33 = 66.66 We use floor to get rid of remainder
    Key_A_C = 0 = 100 - 100 = 0

     */

    PAssert.that(rs)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 0D),
            KV.of(TSTestDataBaseline.KEY_A_B, 2D),
            KV.of(TSTestDataBaseline.KEY_A_C, 100D));
    PAssert.that(rsi)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 100D),
            KV.of(TSTestDataBaseline.KEY_A_B, 66D),
            KV.of(TSTestDataBaseline.KEY_A_C, 0D));

    p.run();
  }

  // @Test
  public final void givenTSDataIsReceivedBeforeWatermarkCalculateSME() {
    // TODO Add once test framework work complete.
  }
}
