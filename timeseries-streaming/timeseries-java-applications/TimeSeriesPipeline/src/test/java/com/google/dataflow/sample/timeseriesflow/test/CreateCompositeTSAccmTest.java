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
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSNumericCombiner;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.transforms.CreateCompositeTSAccum;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation.ComputeType;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.util.Timestamps;
import common.TSTestData;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.Iterator;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CreateCompositeTSAccmTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  private static final String PRICE = "Price";
  private static final String QTY = "QTY";

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

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(5))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    GenerateComputations generateComputations =
        GenerateComputations.builder()
            .setType1FixedWindow(Duration.standardSeconds(5))
            .setType2SlidingWindowDuration(Duration.standardSeconds(5))
            .setType1NumericComputations(ImmutableList.of(new TSNumericCombiner()))
            .setType1KeyMerge(
                ImmutableList.of(
                    CreateCompositeTSAccum.builder()
                        .setKeysToCombineList(
                            ImmutableList.of(
                                TSDataTestUtils.KEY_A_A
                                    .toBuilder()
                                    .setMinorKeyString(PRICE)
                                    .build(),
                                TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString(QTY).build()))
                        .build()))
            .setType2NumericComputations(ImmutableList.of(new VWAPExampleType2(QTY, PRICE)))
            .build();

    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<TSAccum> result = testStream.apply(generateComputations).apply(Values.create());

    PCollection<KV<String, Double>> output1 =
        result
            .apply(
                "Filter A",
                Filter.by(x -> x.getKey().getMinorKeyString().equals(PRICE + "-" + QTY)))
            .apply(
                "MAP A",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                    .via(
                        x ->
                            KV.of(
                                String.join(
                                    "-", x.getKey().getMajorKey(), x.getKey().getMinorKeyString()),
                                new CreateCompositeTSAccmTest.VWAPExampleType2.VWAPBuilder(
                                        x, QTY, PRICE)
                                    .getVWAP()
                                    .getDoubleVal())));

    PCollection<TSKey> output2 =
        result
            .apply(
                "Filter B",
                Filter.by(x -> !x.getKey().getMinorKeyString().equals(PRICE + "-" + QTY)))
            .apply("MAP B", MapElements.into(TypeDescriptor.of(TSKey.class)).via(x -> x.getKey()));

    PAssert.that(output1).containsInAnyOrder(KV.of("Key-A-Price-QTY", 3d));
    PAssert.that(output2)
        .containsInAnyOrder(
            TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString(PRICE).build(),
            TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString(QTY).build(),
            TSDataTestUtils.KEY_B_A.toBuilder().setMinorKeyString(PRICE).build(),
            TSDataTestUtils.KEY_B_A.toBuilder().setMinorKeyString(QTY).build());

    p.run();
  }

  @Test
  /**
   * This is an integration test which will simulate a real computation type Volume Weighted Average
   * Price
   */
  public void testVWAPMultipleKeysExample() throws Exception {

    String resourceName = "TSAccumVWAPTest.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(5))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    GenerateComputations generateComputations =
        GenerateComputations.builder()
            .setType1FixedWindow(Duration.standardSeconds(5))
            .setType2SlidingWindowDuration(Duration.standardSeconds(5))
            .setType1NumericComputations(ImmutableList.of(new TSNumericCombiner()))
            .setType1KeyMerge(
                ImmutableList.of(
                    CreateCompositeTSAccum.builder()
                        .setKeysToCombineList(
                            ImmutableList.of(
                                TSDataTestUtils.KEY_A_A
                                    .toBuilder()
                                    .setMinorKeyString(PRICE)
                                    .build(),
                                TSDataTestUtils.KEY_A_A.toBuilder().setMinorKeyString(QTY).build(),
                                TSDataTestUtils.KEY_B_A
                                    .toBuilder()
                                    .setMinorKeyString(PRICE)
                                    .build(),
                                TSDataTestUtils.KEY_B_A.toBuilder().setMinorKeyString(QTY).build()))
                        .build()))
            .setType2NumericComputations(ImmutableList.of(new VWAPExampleType2(QTY, PRICE)))
            .build();

    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<TSAccum> result = testStream.apply(generateComputations).apply(Values.create());

    PCollection<KV<String, Double>> vwap =
        result
            .apply(
                "Assert-VWAP-Filter-1",
                Filter.by(x -> x.getKey().getMinorKeyString().equals(PRICE + "-" + QTY)))
            .apply(
                "Assert-VWAP-Extract-Value-1",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                    .via(
                        x ->
                            KV.of(
                                String.join(
                                    "-", x.getKey().getMajorKey(), x.getKey().getMinorKeyString()),
                                new VWAPExampleType2.VWAPBuilder(x, QTY, PRICE)
                                    .getVWAP()
                                    .getDoubleVal())));

    PAssert.that(vwap)
        .containsInAnyOrder(KV.of("Key-A-Price-QTY", 3d), KV.of("Key-B-Price-QTY", 3d));

    PCollection<KV<String, String>> dates =
        result
            .apply(
                "Assert-VWAP-Filter-2",
                Filter.by(x -> x.getKey().getMinorKeyString().equals(PRICE + "-" + QTY)))
            .apply(
                "Assert-VWAP-Extract-Value-2",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .via(
                        x ->
                            KV.of(
                                String.join(
                                    "-", x.getKey().getMajorKey(), x.getKey().getMinorKeyString()),
                                String.join(
                                    ",",
                                    Timestamps.toString(x.getLowerWindowBoundary()),
                                    Timestamps.toString(x.getUpperWindowBoundary())))));
    PAssert.that(dates)
        .containsInAnyOrder(
            KV.of("Key-A-Price-QTY", "2000-01-01T00:00:00Z,2000-01-01T00:00:05Z"),
            KV.of("Key-B-Price-QTY", "2000-01-01T00:00:00Z,2000-01-01T00:00:05Z"));
    p.run();
  }

  @TypeTwoComputation(computeType = ComputeType.COMPOSITE_KEY)
  public static class VWAPExampleType2
      extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

    String quantityName;
    String priceName;

    public VWAPExampleType2(String quantityName, String priceName) {
      this.quantityName = quantityName;
      this.priceName = priceName;
    }

    @Override
    public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
      return input.apply("VWAP", ParDo.of(new VWAPTestDoFn(quantityName, priceName)));
    }

    /**
     * A simple test for common use case of a composite TSAccum.
     *
     * <p>The VWAP is the Sum of Min, Max, Last divided by 3 multiply by volume.
     */
    public static class VWAPTestDoFn extends DoFn<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> {

      String quantityName;
      String priceName;

      public VWAPTestDoFn(String quantityName, String priceName) {
        this.quantityName = quantityName;
        this.priceName = priceName;
      }

      @ProcessElement
      public void process(ProcessContext pc, OutputReceiver<KV<TSKey, TSAccum>> o) {

        // TSAccumeSequence is in effect a sequence of ohlc's, need to cycle through for the values
        // that span the time boundary

        Iterator<TSAccum> accums = pc.element().getValue().getAccumsList().iterator();

        BigDecimal cumPriceQty = new BigDecimal(0);
        BigDecimal cumQty = new BigDecimal(0);

        while (accums.hasNext()) {
          TSAccum accum = accums.next();
          VWAPBuilder accumDataMap = new VWAPBuilder(accum, QTY, PRICE);
          if (accumDataMap.getPriceIsHB().getIntVal() == 0
              && accumDataMap.getQuantityIsHB().getIntVal() == 0) {
            BigDecimal price =
                TSDataUtils.add(
                    accumDataMap.getLow(), accumDataMap.getHigh(), accumDataMap.getClose());
            price = price.divide(BigDecimal.valueOf(3), 4);
            BigDecimal qty = TSDataUtils.getBigDecimalFromData(accumDataMap.getQuantity());
            price = price.multiply(qty);
            cumPriceQty = cumPriceQty.add(price);
            cumQty = cumQty.add(qty);
          }
        }

        VWAPBuilder output =
            new VWAPBuilder(TSAccum.newBuilder().setKey(pc.element().getKey()).build(), QTY, PRICE);

        output.setVWAP(
            Data.newBuilder().setDoubleVal(cumPriceQty.divide(cumQty, 4).doubleValue()).build());

        o.output(KV.of(pc.element().getKey(), output.build()));
      }

      /** Helper for working with the Map data within the accum. */
    }

    public static class VWAPBuilder extends AccumCoreMetadataBuilder {

      private final String quantityName;
      private final String priceName;

      public VWAPBuilder(TSAccum tsAccum, String quantityName, String priceName) {
        super(tsAccum);
        this.quantityName = quantityName;
        this.priceName = priceName;
      }

      public Data getQuantity() {
        return getValueOrNull(quantityName + "-" + Indicators.SUM.name());
      }

      public Data getHigh() {
        return getValueOrNull(priceName + "-" + Indicators.MAX.name());
      }

      public Data getLow() {
        return getValueOrNull(priceName + "-" + Indicators.MIN.name());
      }

      public Data getClose() {
        return getValueOrNull(priceName + "-" + Indicators.LAST.name());
      }

      public Data getPriceIsHB() {
        return getValueOrNull(priceName + "-" + "hb");
      }

      public Data getQuantityIsHB() {
        return getValueOrNull(quantityName + "-" + "hb");
      }

      public Data getVWAP() {
        return getValueOrNull("VWAP");
      }

      public void setVWAP(Data data) {
        setValue("VWAP", data);
      }
    }
  }
}
