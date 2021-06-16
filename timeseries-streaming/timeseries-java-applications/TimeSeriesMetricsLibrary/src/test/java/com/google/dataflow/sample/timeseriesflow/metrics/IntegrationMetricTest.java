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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn.AccumVWAPBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn.VWAPOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.rule.ValueInBoundsGFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.rule.ValueInBoundsGFn.ValueInBoundsOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Max;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Min;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Sum;
import com.google.dataflow.sample.timeseriesflow.test.TSDataTestUtils;
import com.google.gson.stream.JsonReader;
import common.TSTestData;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Filter;
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

public class IntegrationMetricTest {

  static final TSKey PRIMARY_MINOR_KEY =
      TSKey.newBuilder().setMajorKey("MajorKey").setMinorKeyString("PRICE").build();
  static final TSKey LOWER_MINOR_KEY =
      TSKey.newBuilder().setMajorKey("MajorKey").setMinorKeyString("BID").build();
  static final TSKey UPPER_MINOR_KEY =
      TSKey.newBuilder().setMajorKey("MajorKey").setMinorKeyString("ASK").build();

  static final String PRIMARY_METRIC = "VWAP";

  static final Instant START = Instant.parse("2000-01-01T00:00:00Z");

  private static final String PRICE = "PRICE";

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  @Test
  public void testVWAPWithinBounds() throws Exception {

    String resourceName = "VWAPWithinBoundTest.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    List<String> majorKeys =
        ImmutableList.of(
            TSDataTestUtils.KEY_A_A.getMajorKey(), TSDataTestUtils.KEY_B_A.getMajorKey());

    VWAPOptions vwapOptions = p.getOptions().as(VWAPOptions.class);
    vwapOptions.setVWAPMajorKeyName(majorKeys);
    vwapOptions.setVWAPPriceName(PRICE);
    vwapOptions.setTypeOneComputationsLengthInSecs(5);
    vwapOptions.setTypeTwoComputationsLengthInSecs(5);

    ValueInBoundsOptions options = vwapOptions.as(ValueInBoundsOptions.class);

    options.setValueInBoundsPrimaryMinorKeyName(PRIMARY_MINOR_KEY.getMinorKeyString());
    options.setValueInBoundsPrimaryMetricName(PRIMARY_METRIC);

    options.setValueInBoundsLowerBoundaryMinorKeyName(LOWER_MINOR_KEY.getMinorKeyString());
    options.setValueInBoundsUpperBoundaryMinorKeyName(UPPER_MINOR_KEY.getMinorKeyString());

    options.setValueInBoundsOutputMetricName("OUTPUT");

    options.setValueInBoundsMajorKeyName(majorKeys);

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(5))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    options.setGapFillEnabled(true);
    options.setAbsoluteStopTimeMSTimestamp(NOW.plus(Duration.standardSeconds(15)).getMillis());
    options.setEnableHoldAndPropogateLastValue(true);

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(vwapOptions)

            //            .setPerfectRectangles(
            //                PerfectRectangles.withWindowAndAbsoluteStop(
            //
            // Duration.standardSeconds(vwapOptions.getTypeOneComputationsLengthInSecs()),
            //                        NOW.plus(Duration.standardSeconds(15)))
            //                    .enablePreviousValueFill())
            .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
            .setComplexType2Metrics(ImmutableList.of(VWAPGFn.class))
            .setPostProcessRules(ValueInBoundsGFn.class)
            .build();

    /*
        Key A - 2000-01-01T00:00:00Z PriceVol 10.0
        Key A - 2000-01-01T00:00:00Z QTY 2.0
        VWAP 2000-01-01T00:00:00Z-> 2000-01-01T00:00:05Z = (10*2)/2 = 10


        Key - A GapFill
        ASK 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = 8
        BID 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = 6
        VWAP 2000-01-01T00:00:05Z-> 2000-01-01T00:00:10Z = ZERO
        OUTPUT Check if last VWAP ( 10 ) is within 8-6 bound = 8

        Key A - 2000-01-01T00:00:11Z PriceVol 10.0
        Key A - 2000-01-01T00:00:11Z QTY 2.0
        VWAP 2000-01-01T00:00:10Z-> 2000-01-01T00:00:15Z = (10*2)/2 = 10

    */

    PCollection<TSAccum> testStream =
        p.apply(stream)
            .apply(generateComputations)
            .apply(Values.create())
            .apply(Filter.by(x -> x.getKey().getMinorKeyString().equals(PRICE)));

    String outputName = options.getValueInBoundsOutputMetricName();

    PCollection<String> vwapresult =
        testStream.apply(
            "MapElement_T1",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    x ->
                        x.getKey().getMajorKey()
                            + "::"
                            + Optional.ofNullable(x.getDataStoreMap().get(AccumVWAPBuilder.VWAP))
                                .orElse(CommonUtils.createNumData(999D))
                                .getDoubleVal()));

    PCollection<String> ruleResult =
        testStream.apply(
            "MapElement_T2",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    x ->
                        x.getKey().getMajorKey()
                            + "::"
                            + outputName
                            + "::"
                            + Optional.ofNullable(x.getDataStoreMap().get(outputName))
                                .orElse(CommonUtils.createNumData(999D))
                                .getDoubleVal()));

    PAssert.that(vwapresult)
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:00Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:05Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::0.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:10Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0");

    PAssert.that(ruleResult)
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:00Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::OUTPUT::10.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:05Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::OUTPUT::8.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:10Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::OUTPUT::10.0");

    p.run();
  }
}
