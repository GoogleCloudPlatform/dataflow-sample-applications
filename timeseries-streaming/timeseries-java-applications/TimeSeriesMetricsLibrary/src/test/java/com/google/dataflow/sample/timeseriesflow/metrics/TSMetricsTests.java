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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSNumericCombiner;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.GenerateComputations;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class TSMetricsTests {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  /* Simple test to check RSI Technical is created correctly */
  public void testCreateRSI() {

    // Key A-A will increase, Key A-B will decrease, Key A-C will remain stationary
    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.START))
            .addElements(TSTestDataBaseline.DOUBLE_POINT_1_A_A)
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_2_A_B
                    .toBuilder()
                    .setData(CommonUtils.createNumData(0D))
                    .build())
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_3_A_C
                    .toBuilder()
                    .setData(CommonUtils.createNumData(7D))
                    .build())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.PLUS_FIVE_SECS))
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_2_A_A
                    .toBuilder()
                    .setData(CommonUtils.createNumData(4D))
                    .build())
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_2_A_B
                    .toBuilder()
                    .setData(CommonUtils.createNumData(12D))
                    .build())
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_2_A_C
                    .toBuilder()
                    .setData(CommonUtils.createNumData(4D))
                    .build())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.PLUS_TEN_SECS))
            // Mutate final value so we have div with no remainder
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_3_A_A
                    .toBuilder()
                    .setData(CommonUtils.createNumData(7D))
                    .build())
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_2_A_B
                    .toBuilder()
                    .setData(CommonUtils.createNumData(6D))
                    .build())
            .addElements(
                TSTestDataBaseline.DOUBLE_POINT_1_A_C
                    .toBuilder()
                    .setData(CommonUtils.createNumData(1D))
                    .build())
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setType1NumericComputations(ImmutableList.of(new TSNumericCombiner()))
                    .setType2NumericComputations(
                        ImmutableList.of(
                            RSI.toBuilder()
                                .setAverageComputationMethod(RSI.AverageComputationMethod.ALL)
                                .build()
                                .create()))
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

  @Test
  /* Simple test to check Simple Moving Average Technical is created correctly */
  public void testCreateSMA() throws IOException {

    String resourceName = "TSTestData.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(15))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setType1NumericComputations(ImmutableList.of(new TSNumericCombiner()))
                    .setType2NumericComputations(
                        ImmutableList.of(
                            MA.toBuilder()
                                .setAverageComputationMethod(
                                    MA.AverageComputationMethod.SIMPLE_MOVING_AVERAGE)
                                .build()
                                .create()))
                    .build());

    // The sliding window will create partial values, to keep testing simple we just test
    // correctness of SMA for the full value

    PCollection<KV<TSKey, TSAccum>> fullAccum =
        techAccum.apply(
            Filter.by(
                x ->
                    x.getValue()
                            .getDataStoreOrThrow(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name())
                            .getIntVal()
                        == 3));

    PCollection<KV<TSKey, Double>> sma =
        fullAccum.apply(
            "SMA",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            Math.floor(
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.SIMPLE_MOVING_AVERAGE.name())
                                    .getDoubleVal()))));

    /*
    SMA Sum / Count
    SMA Key_A_A = 1 + 1 + 3 + 3 + 8 + 8 / 6 = 4
    SMA Key_A_B = 16 + 12 + 8 / 3 = 12
    SMA Key_A_C = 12 + 12 + 12 / 3 = 12

     */
    PAssert.that(sma)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 4D),
            KV.of(TSTestDataBaseline.KEY_A_B, 12D),
            KV.of(TSTestDataBaseline.KEY_A_C, 12D));

    p.run();
  }

  @Test
  /* Simple test to check Exponential Moving Average Technical is created correctly */
  public void testCreateEMA() throws IOException {

    String resourceName = "TSTestDataHints.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(15))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setType1NumericComputations(ImmutableList.of(new TSNumericCombiner()))
                    .setType2NumericComputations(
                        ImmutableList.of(
                            MA.toBuilder()
                                .setAverageComputationMethod(
                                    MA.AverageComputationMethod.EXPONENTIAL_MOVING_AVERAGE)
                                .setWeight(BigDecimal.valueOf(2D / (3D + 1D)))
                                .build()
                                .create()))
                    .build());

    // The sliding window will create partial values, to keep testing simple we just test
    // correctness of EMA for the full value

    PCollection<KV<TSKey, TSAccum>> fullAccum =
        techAccum.apply(
            Filter.by(
                x ->
                    x.getValue()
                            .getDataStoreOrThrow(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name())
                            .getIntVal()
                        == 3));

    PCollection<KV<TSKey, Double>> ema =
        fullAccum.apply(
            "EMA",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            x.getValue()
                                .getDataStoreOrThrow(
                                    FsiTechnicalIndicators.EXPONENTIAL_MOVING_AVERAGE.name())
                                .getDoubleVal())));

    /*
    EMA EMA_n = WeightedSum_n / WeightedCount_n
    EMA Key_A_A = [1, 3, 8] = 5.571429
    EMA Key_A_B = [16, 12, 8] = 10.285714
    EMA Key_A_C = [12, 12, 12] = 12

     */
    PAssert.that(ema)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 5.571D),
            KV.of(TSTestDataBaseline.KEY_A_B, 10.286D),
            KV.of(TSTestDataBaseline.KEY_A_C, 12D));

    p.run();
  }

  @Test
  /* Simple test to check Exponential Moving Average Technical is created correctly */
  public void testCreateBB() throws IOException {

    String resourceName = "TSTestDataHints.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(15))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setType1NumericComputations(ImmutableList.of(new TSNumericCombiner()))
                    .setType2NumericComputations(
                        ImmutableList.of(
                            BB.toBuilder()
                                .setAverageComputationMethod(
                                    BB.AverageComputationMethod.SIMPLE_MOVING_AVERAGE)
                                .setDevFactor(2)
                                .build()
                                .create()))
                    .build());

    // The sliding window will create partial values, to keep testing simple we just test
    // correctness of BB for the full value

    PCollection<KV<TSKey, TSAccum>> fullAccum =
        techAccum.apply(
            Filter.by(
                x ->
                    x.getValue()
                            .getDataStoreOrThrow(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name())
                            .getIntVal()
                        == 3));

    PCollection<KV<TSKey, List<Double>>> bollingerBand =
        fullAccum.apply(
            "BB",
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(TSKey.class),
                        TypeDescriptors.lists(TypeDescriptors.doubles())))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            Arrays.asList(
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.BB_MIDDLE_BAND_SMA.name())
                                    .getDoubleVal(),
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.BB_UPPER_BAND_SMA.name())
                                    .getDoubleVal(),
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.BB_BOTTOM_BAND_SMA.name())
                                    .getDoubleVal()))));

    /*
    BB MIDDLE_BAND = SMA or EMA, UPPER_BAND = MIDDLE_BAND + dev_factor * STDDEV, BOTTOM_BAND = MIDDLE_BAND - dev_factor * STDDEV
    BB SMA Key_A_A = [1, 3, 8] = [4.0, 9.8878405776, -1.8878405776]
    BB SMA Key_A_B = [16, 12, 8] = [12.0, 18.5319726474, 5.4680273526]
    BB SMA Key_A_C = [12, 12, 12] = [12, 12, 12]

     */
    PAssert.that(bollingerBand)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, Arrays.asList(4.0D, 9.8878405776, -1.8878405776D)),
            KV.of(TSTestDataBaseline.KEY_A_B, Arrays.asList(12.0D, 18.5319726474D, 5.4680273526D)),
            KV.of(TSTestDataBaseline.KEY_A_C, Arrays.asList(12D, 12D, 12D)));

    p.run();
  }
}
