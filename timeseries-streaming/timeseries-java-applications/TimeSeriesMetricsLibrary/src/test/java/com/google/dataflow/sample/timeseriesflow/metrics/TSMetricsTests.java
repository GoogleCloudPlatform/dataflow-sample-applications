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

import static com.google.dataflow.sample.timeseriesflow.test.TestUtils.timestampedValueFromTSDataPoint;

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.rsi.RSIGFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Max;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Min;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Sum;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.logrtn.LogRtnFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma.MAFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma.MAFn.MAAvgComputeMethod;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma.MAFn.MAOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.stddev.StdDevFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.sumupdown.SumUpDownFn;
import com.google.dataflow.sample.timeseriesflow.test.TSDataTestUtils;
import com.google.gson.stream.JsonReader;
import common.TSTestData;
import common.TSTestDataBaseline;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
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
  public void testCreateSumUpDown() {

    // Key A-A will increase, Key A-B will decrease, Key A-C will remain stationary
    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.START))
            .addElements(timestampedValueFromTSDataPoint(TSTestDataBaseline.DOUBLE_POINT_1_A_A))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_1_A_B
                        .toBuilder()
                        .setData(CommonUtils.createNumData(0D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_1_A_C
                        .toBuilder()
                        .setData(CommonUtils.createNumData(7D))
                        .build()))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.PLUS_FIVE_SECS))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_2_A_A
                        .toBuilder()
                        .setData(CommonUtils.createNumData(4D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_2_A_B
                        .toBuilder()
                        .setData(CommonUtils.createNumData(12D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_2_A_C
                        .toBuilder()
                        .setData(CommonUtils.createNumData(4D))
                        .build()))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.PLUS_TEN_SECS))
            // Mutate final value so we have div with no remainder
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_3_A_A
                        .toBuilder()
                        .setData(CommonUtils.createNumData(7D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_3_A_B
                        .toBuilder()
                        .setData(CommonUtils.createNumData(6D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_3_A_C
                        .toBuilder()
                        .setData(CommonUtils.createNumData(1D))
                        .build()))
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setEnableGapFill(false)
                    .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
                    .setBasicType2Metrics(ImmutableList.of(SumUpDownFn.class))
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

    PCollection<KV<TSKey, Double>> sumUp =
        fullAccum.apply(
            "SumUP",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            x.getValue()
                                .getDataStoreOrThrow(FsiTechnicalIndicators.SUM_UP_MOVEMENT.name())
                                .getDoubleVal())));

    PCollection<KV<TSKey, Double>> sumDown =
        fullAccum.apply(
            "SumDown",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            x.getValue()
                                .getDataStoreOrThrow(
                                    FsiTechnicalIndicators.SUM_DOWN_MOVEMENT.name())
                                .getDoubleVal())));

    /*
    AvgG / AvgL
    Key_A_A = AvgGain 6 AvgLoss 0
    Key_A_B = AvgGain 12 AvgLoss -6
    Key_A_C = AvgGain 0 AvgLoss -6

     */

    PAssert.that(sumUp)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 6D),
            KV.of(TSTestDataBaseline.KEY_A_B, 12D),
            KV.of(TSTestDataBaseline.KEY_A_C, 0D));
    PAssert.that(sumDown)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 0D),
            KV.of(TSTestDataBaseline.KEY_A_B, -6D),
            KV.of(TSTestDataBaseline.KEY_A_C, -6D));

    p.run();
  }

  @Test
  /* Simple test to check RSI Technical is created correctly */
  public void testCreateRSI() {

    // Key A-A will increase, Key A-B will decrease, Key A-C will remain stationary
    TestStream<TSDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSDataPoint.class))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.START))
            .addElements(timestampedValueFromTSDataPoint(TSTestDataBaseline.DOUBLE_POINT_1_A_A))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_1_A_B
                        .toBuilder()
                        .setData(CommonUtils.createNumData(0D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_1_A_C
                        .toBuilder()
                        .setData(CommonUtils.createNumData(7D))
                        .build()))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.PLUS_FIVE_SECS))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_2_A_A
                        .toBuilder()
                        .setData(CommonUtils.createNumData(4D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_2_A_B
                        .toBuilder()
                        .setData(CommonUtils.createNumData(12D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_2_A_C
                        .toBuilder()
                        .setData(CommonUtils.createNumData(4D))
                        .build()))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSTestDataBaseline.PLUS_TEN_SECS))
            // Mutate final value so we have div with no remainder
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_3_A_A
                        .toBuilder()
                        .setData(CommonUtils.createNumData(7D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_3_A_B
                        .toBuilder()
                        .setData(CommonUtils.createNumData(6D))
                        .build()))
            .addElements(
                timestampedValueFromTSDataPoint(
                    TSTestDataBaseline.DOUBLE_POINT_3_A_C
                        .toBuilder()
                        .setData(CommonUtils.createNumData(1D))
                        .build()))
            .advanceWatermarkToInfinity();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setEnableGapFill(false)
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

  @Test
  /* Simple test to check Simple Moving Average Technical is created correctly */
  public void testCreateSMA() throws IOException {

    MAOptions options = p.getOptions().as(MAOptions.class);
    options.setMAAvgComputeAlpha(BigDecimal.valueOf(2D / (3D + 1D)).doubleValue());
    options.setMAAvgComputeMethod(MAAvgComputeMethod.SIMPLE_MOVING_AVERAGE);

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
                    .setEnableGapFill(false)
                    .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
                    .setBasicType2Metrics(ImmutableList.of(MAFn.class))
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

    MAOptions options = p.getOptions().as(MAOptions.class);
    options.setMAAvgComputeAlpha(BigDecimal.valueOf(2D / (3D + 1D)).doubleValue());
    options.setMAAvgComputeMethod(MAAvgComputeMethod.EXPONENTIAL_MOVING_AVERAGE);

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setEnableGapFill(false)
                    .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
                    .setBasicType2Metrics(ImmutableList.of(MAFn.class))
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
  public void testCreateStdDev() throws IOException {

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
                    .setEnableGapFill(false)
                    .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
                    .setBasicType2Metrics(ImmutableList.of(StdDevFn.class))
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

    PCollection<KV<TSKey, Double>> stdDev =
        fullAccum.apply(
            "StdDev",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            x.getValue()
                                .getDataStoreOrThrow(
                                    FsiTechnicalIndicators.STANDARD_DEVIATION.name())
                                .getDoubleVal())));

    /*
    POPULATION STDDEV = SQRT(MEAN_SQUARED - SQUARED_MEAN)
    Key_A_A = [1, 3, 8] = [2.9439202888]
    Key_A_B = [16, 12, 8] = [3.2659863237]
    Key_A_C = [12, 12, 12] = [0]

     */
    PAssert.that(stdDev)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 2.9439202888D),
            KV.of(TSTestDataBaseline.KEY_A_B, 3.2659863237D),
            KV.of(TSTestDataBaseline.KEY_A_C, 0D));

    p.run();
  }

  @Test
  /* Simple test to check Exponential Moving Average Technical is created correctly */
  @SuppressWarnings("unchecked")
  public void testCreateLogRtn() throws IOException {

    String resourceName = "LogRtnTSTestDataHints.json";
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
                    .setEnableGapFill(false)
                    .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
                    .setBasicType2Metrics(ImmutableList.of(LogRtnFn.class))
                    .build());

    // The sliding window will create partial values, to keep testing simple we just test
    // correctness of BB for the full value

    PCollection<KV<TSKey, Double>> logRtn =
        techAccum.apply(
            "LogRtn",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptors.doubles()))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            new BigDecimal(
                                    x.getValue()
                                        .getDataStoreOrThrow(FsiTechnicalIndicators.LOG_RTN.name())
                                        .getDoubleVal())
                                .setScale(4, RoundingMode.CEILING)
                                .doubleValue())));

    /*
    LogRtn = Log(lead-lag)
    Key_A_A = [1, 3] = [1.0987]
    Key_A_B = [16, 12] = [-0.2876]
    Key_A_C = [12, 12] = [0]
    Key_A_C = [12, 0] = [0]

    Key_A_A = [1, 3, 8] = [0.9808]
    Key_A_B = [16, 12, 8] = [-0.4054]
    Key_A_C = [12, 12, 12] = [0]
    Key_A_C = [12, 0, 12] = [0]


     */
    TSKey keyAC = TSTestDataBaseline.KEY_A_C.toBuilder().setMinorKeyString("MKey-d").build();

    PAssert.that(logRtn)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(5)))
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 0D),
            KV.of(TSTestDataBaseline.KEY_A_B, 0D),
            KV.of(TSTestDataBaseline.KEY_A_C, 0D),
            KV.of(keyAC, 0D))
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 5000), Duration.standardSeconds(5)))
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 1.0987),
            KV.of(TSTestDataBaseline.KEY_A_B, -0.2876),
            KV.of(TSTestDataBaseline.KEY_A_C, 0D),
            KV.of(keyAC, 0D))
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 10000), Duration.standardSeconds(5)))
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, 0.9809),
            KV.of(TSTestDataBaseline.KEY_A_B, -0.4054),
            KV.of(TSTestDataBaseline.KEY_A_C, 0D),
            KV.of(keyAC, 0D));

    p.run();
  }
}
