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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn.AccumVWAPBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn.VWAPOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.*;
import com.google.dataflow.sample.timeseriesflow.test.TSDataTestUtils;
import com.google.gson.stream.JsonReader;
import common.TSTestData;
import java.io.File;
import java.io.FileReader;
import java.util.Optional;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VWAPGraphFnTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  private static final String PRICE = "PRICE";

  @Test
  /**
   * This is an integration test which will simulate a real computation type Volume Weighted Average
   * Price
   */
  public void testVWAPExample() throws Exception {

    String resourceName = "TSAccumVWAPTest.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    VWAPOptions vwapOptions = p.getOptions().as(VWAPOptions.class);
    vwapOptions.setVWAPMajorKeyName(
        ImmutableList.of(
            TSDataTestUtils.KEY_A_A.getMajorKey(), TSDataTestUtils.KEY_B_A.getMajorKey()));
    vwapOptions.setVWAPPriceName(PRICE);
    vwapOptions.setTypeOneComputationsLengthInSecs(5);
    vwapOptions.setTypeTwoComputationsLengthInSecs(5);

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(5))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(vwapOptions)
            .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
            .setComplexType2Metrics(ImmutableList.of(VWAPGFn.class))
            .build();

    /*
        Key A - 2000-01-01T00:00:00Z PriceVol 10.0
        Key A - 2000-01-01T00:00:00Z QTY 2.0
        VWAP 2000-01-01T00:00:00Z-> 2000-01-01T00:00:05Z = (10*2)/2 = 10

        Key A - 2000-01-01T00:00:05Z PriceVol 10.0
        Key A - 2000-01-01T00:00:05Z QTY 2.0
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = (10*2)/2 = 10

        Key B - 2000-01-01T00:00:00Z PriceVol 20.0
        Key B - 2000-01-01T00:00:00Z QTY 2.0
        Key B - 2000-01-01T00:00:01Z PriceVol 40.0
        Key B - 2000-01-01T00:00:01Z QTY 8.0
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = (20*2)+(40*10)/12 = 36

    */

    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<String> result =
        testStream
            .apply(generateComputations)
            .apply(Values.create())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMajorKey()
                                + "::"
                                + Optional.ofNullable(
                                        x.getDataStoreMap().get(AccumVWAPBuilder.VWAP))
                                    .orElse(CommonUtils.createNumData(0D))
                                    .getDoubleVal()));

    PAssert.that(result)
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:00Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0", "Key-B::36.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:05Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:10Z"), Duration.standardSeconds(5)))
        .empty();

    p.run();
  }

  @Test
  public void testVWAPZeroForGapFillWithoutBackFill() throws Exception {
    String resourceName = "VWAPTestGap.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    VWAPOptions vwapOptions = p.getOptions().as(VWAPOptions.class);
    vwapOptions.setVWAPMajorKeyName(
        ImmutableList.of(
            TSDataTestUtils.KEY_A_A.getMajorKey(), TSDataTestUtils.KEY_B_A.getMajorKey()));
    vwapOptions.setVWAPPriceName(PRICE);
    vwapOptions.setTypeOneComputationsLengthInSecs(5);
    vwapOptions.setTypeTwoComputationsLengthInSecs(5);

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(5))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    vwapOptions.setGapFillEnabled(true);
    vwapOptions.setAbsoluteStopTimeMSTimestamp(NOW.plus(Duration.standardSeconds(15)).getMillis());
    vwapOptions.setEnableHoldAndPropogateLastValue(true);

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(vwapOptions)
            .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
            .setComplexType2Metrics(ImmutableList.of(VWAPGFn.class))
            .build();

    /*
        Key A - 2000-01-01T00:00:00Z PriceVol 10.0
        Key A - 2000-01-01T00:00:00Z QTY 2.0
        VWAP 2000-01-01T00:00:00Z-> 2000-01-01T00:00:05Z = (10*2)/2 = 10

        Key B - 2000-01-01T00:00:00Z PriceVol 20.0
        Key B - 2000-01-01T00:00:00Z QTY 2.0
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = (20*2)/2 = 20

        Key - A GapFill
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = ZERO

        Key - B GapFill
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = ZERO

        Key A - 2000-01-01T00:00:11Z PriceVol 10.0
        Key A - 2000-01-01T00:00:11Z QTY 2.0
        VWAP 2000-01-01T00:00:10Z-> 2000-01-01T00:00:15Z = (10*2)/2 = 10

        Key B - 2000-01-01T00:00:11Z PriceVol 40.0
        Key B - 2000-01-01T00:00:11Z QTY 8.0
        VWAP 2000-01-01T00:00:10Z-> 2000-01-01T00:00:15Z = (40*8)/8 = 40

    */
    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<String> result =
        testStream
            .apply(generateComputations)
            .apply(Values.create())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMajorKey()
                                + "::"
                                + Optional.ofNullable(
                                        x.getDataStoreMap().get(AccumVWAPBuilder.VWAP))
                                    .orElse(CommonUtils.createNumData(0D))
                                    .getDoubleVal()));

    PAssert.that(result)
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:00Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0", "Key-B::20.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:05Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::0.0", "Key-B::0.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:10Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0", "Key-B::40.0");

    p.run();
  }

  @Test
  public void testVWAPZeroForGapFillWithBackFill() throws Exception {
    String resourceName = "VWAPTestGap.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    VWAPOptions vwapOptions = p.getOptions().as(VWAPOptions.class);
    vwapOptions.setVWAPMajorKeyName(
        ImmutableList.of(
            TSDataTestUtils.KEY_A_A.getMajorKey(), TSDataTestUtils.KEY_B_A.getMajorKey()));
    vwapOptions.setVWAPPriceName(PRICE);
    vwapOptions.setTypeOneComputationsLengthInSecs(5);
    vwapOptions.setTypeTwoComputationsLengthInSecs(5);

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(5))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    vwapOptions.setGapFillEnabled(true);
    vwapOptions.setAbsoluteStopTimeMSTimestamp(NOW.plus(Duration.standardSeconds(15)).getMillis());
    vwapOptions.setEnableHoldAndPropogateLastValue(true);

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(vwapOptions)
            .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
            .setComplexType2Metrics(ImmutableList.of(VWAPGFn.class))
            .build();

    /*
        Key A - 2000-01-01T00:00:00Z PriceVol 10.0
        Key A - 2000-01-01T00:00:00Z QTY 2.0
        VWAP 2000-01-01T00:00:00Z-> 2000-01-01T00:00:05Z = (10*2)/2 = 10

        Key B - 2000-01-01T00:00:00Z PriceVol 20.0
        Key B - 2000-01-01T00:00:00Z QTY 2.0
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = (20*2)/2 = 20

        Key - A GapFill
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = ZERO

        Key - B GapFill
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = ZERO

        Key A - 2000-01-01T00:00:11Z PriceVol 10.0
        Key A - 2000-01-01T00:00:11Z QTY 2.0
        VWAP 2000-01-01T00:00:10Z-> 2000-01-01T00:00:15Z = (10*2)/2 = 10

        Key B - 2000-01-01T00:00:11Z PriceVol 40.0
        Key B - 2000-01-01T00:00:11Z QTY 8.0
        VWAP 2000-01-01T00:00:10Z-> 2000-01-01T00:00:15Z = (40*8)/8 = 40

    */
    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<String> result =
        testStream
            .apply(generateComputations)
            .apply(Values.create())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMajorKey()
                                + "::"
                                + Optional.ofNullable(
                                        x.getDataStoreMap().get(AccumVWAPBuilder.VWAP))
                                    .orElse(CommonUtils.createNumData(999D))
                                    .getDoubleVal()));

    PAssert.that(result)
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:00Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0", "Key-B::20.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:05Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::0.0", "Key-B::0.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:10Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0", "Key-B::40.0");

    p.run();
  }
}
