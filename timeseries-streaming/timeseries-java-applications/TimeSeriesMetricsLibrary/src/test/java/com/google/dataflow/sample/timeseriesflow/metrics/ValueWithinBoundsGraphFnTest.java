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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.rule.ValueInBoundsGFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.rule.ValueInBoundsGFn.ValueInBoundsOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ValueWithinBoundsGraphFnTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  static final TSKey PRIMARY_MINOR_KEY =
      TSKey.newBuilder().setMajorKey("MajorKey").setMinorKeyString("PRIMARY").build();
  static final TSKey LOWER_MINOR_KEY =
      TSKey.newBuilder().setMajorKey("MajorKey").setMinorKeyString("BID").build();
  static final TSKey UPPER_MINOR_KEY =
      TSKey.newBuilder().setMajorKey("MajorKey").setMinorKeyString("ASK").build();

  static final String PRIMARY_METRIC = "VWAP";
  static final String LOWER_METRIC = "LAST";
  static final String UPPER_METRIC = "LAST";

  static final Instant START = Instant.parse("2000-01-01T00:00:00Z");

  static final Iterable<KV<TSKey, TSAccum>> valueBetween =
      ImmutableList.of(
          KV.of(
              PRIMARY_MINOR_KEY,
              TSAccum.newBuilder()
                  .putDataStore(PRIMARY_METRIC, CommonUtils.createNumData(0))
                  .build()),
          KV.of(
              LOWER_MINOR_KEY,
              TSAccum.newBuilder()
                  .putDataStore(LOWER_METRIC, CommonUtils.createNumData(-5))
                  .build()),
          KV.of(
              UPPER_MINOR_KEY,
              TSAccum.newBuilder()
                  .putDataStore(UPPER_METRIC, CommonUtils.createNumData(5))
                  .build()));

  @Test
  public void testWithinStread() throws Exception {
    Data primary = CommonUtils.createNumData(0);
    Data lower = CommonUtils.createNumData(-5);
    Data upper = CommonUtils.createNumData(10);
    Assert.assertEquals(
        Data.newBuilder().setIntVal(0).build(),
        ValueInBoundsGFn.checkInSpread(primary, lower, upper));
  }

  @Test
  public void testLessThanSpread() throws Exception {
    Data primary = CommonUtils.createNumData(-10);
    Data lower = CommonUtils.createNumData(-5);
    Data upper = CommonUtils.createNumData(10);
    Assert.assertEquals(
        Data.newBuilder().setIntVal(-5).build(),
        ValueInBoundsGFn.checkInSpread(primary, lower, upper));
  }

  @Test
  public void testHigherThanStread() throws Exception {
    Data primary = CommonUtils.createNumData(20);
    Data lower = CommonUtils.createNumData(-5);
    Data upper = CommonUtils.createNumData(10);
    Assert.assertEquals(
        Data.newBuilder().setIntVal(10).build(),
        ValueInBoundsGFn.checkInSpread(primary, lower, upper));
  }

  @Test
  public void testDifferentTypes() throws Exception {
    Data primary = CommonUtils.createNumData(20);
    Data lower = CommonUtils.createNumData(-5);
    Data upper = CommonUtils.createNumData(10D);
    Assert.assertEquals(
        Data.newBuilder().setDoubleVal(10D).build(),
        ValueInBoundsGFn.checkInSpread(primary, lower, upper));
  }

  public ValueInBoundsOptions testOptions(Pipeline pipeline) {

    ValueInBoundsOptions options = pipeline.getOptions().as(ValueInBoundsOptions.class);

    options.setValueInBoundsPrimaryMinorKeyName(PRIMARY_MINOR_KEY.getMinorKeyString());
    options.setValueInBoundsPrimaryMetricName(PRIMARY_METRIC);
    options.setValueInBoundsLowerBoundaryMinorKeyName(LOWER_MINOR_KEY.getMinorKeyString());
    options.setValueInBoundsUpperBoundaryMinorKeyName(UPPER_MINOR_KEY.getMinorKeyString());

    options.setValueInBoundsOutputMetricName("OUTPUT");

    options.setValueInBoundsMajorKeyName(ImmutableList.of("MajorKey"));

    return options;
  }

  @Test
  public void testBasic() throws Exception {

    ValueInBoundsGFn valueWithinBoundsGraphFn = new ValueInBoundsGFn();

    ValueInBoundsOptions options = testOptions(p);

    PCollection<KV<TSKey, TSAccum>> values =
        p.apply(
            Create.of(
                    KV.of(
                        LOWER_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(LOWER_MINOR_KEY)
                            .putDataStore(LOWER_METRIC, CommonUtils.createNumData(-5))
                            .build()),
                    KV.of(
                        UPPER_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(UPPER_MINOR_KEY)
                            .putDataStore(UPPER_METRIC, CommonUtils.createNumData(5))
                            .build()),
                    KV.of(
                        PRIMARY_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(PRIMARY_MINOR_KEY)
                            .putDataStore(PRIMARY_METRIC, CommonUtils.createNumData(2))
                            .build()))
                .withCoder(CommonUtils.getKvTSAccumCoder()));

    String outputName = options.getValueInBoundsOutputMetricName();

    PCollection<String> result =
        values
            .apply(new ValueInBoundsGFn())
            .apply(
                Filter.by(
                    x ->
                        x.getKey()
                            .getMinorKeyString()
                            .equals(PRIMARY_MINOR_KEY.getMinorKeyString())))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMinorKeyString()
                                + "::"
                                + outputName
                                + "::"
                                + x.getValue()
                                    .getDataStoreOrDefault(
                                        outputName, Data.newBuilder().setIntVal(999).build())
                                    .getIntVal()));

    PAssert.that(result).containsInAnyOrder("PRIMARY::OUTPUT::2");

    p.run();
  }

  @Test
  /**
   * This test will confirm that no primary is output is none is input, but all other values pass
   * through.
   */
  public void testNoPrimary() throws Exception {

    ValueInBoundsGFn valueWithinBoundsGraphFn = new ValueInBoundsGFn();

    ValueInBoundsOptions options = testOptions(p);

    PCollection<KV<TSKey, TSAccum>> values =
        p.apply(
            Create.of(
                    KV.of(
                        UPPER_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(UPPER_MINOR_KEY)
                            .putDataStore(UPPER_METRIC, CommonUtils.createNumData(5))
                            .build()))
                .withCoder(CommonUtils.getKvTSAccumCoder()));

    String outputName = options.getValueInBoundsOutputMetricName();

    PCollection<String> result =
        values
            .apply(new ValueInBoundsGFn())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMinorKeyString()
                                + "::"
                                + UPPER_METRIC
                                + "::"
                                + x.getValue()
                                    .getDataStoreOrDefault(
                                        UPPER_METRIC, Data.newBuilder().setIntVal(999).build())
                                    .getIntVal()));

    PAssert.that(result)
        .containsInAnyOrder(
            String.format("%s::%s::5", UPPER_MINOR_KEY.getMinorKeyString(), UPPER_METRIC));

    p.run();
  }

  @Test
  public void testNoLowerFound() throws Exception {

    ValueInBoundsGFn valueWithinBoundsGraphFn = new ValueInBoundsGFn();

    ValueInBoundsOptions options = testOptions(p);

    PCollection<KV<TSKey, TSAccum>> values =
        p.apply(
            Create.of(
                    KV.of(
                        UPPER_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(UPPER_MINOR_KEY)
                            .putDataStore(UPPER_METRIC, CommonUtils.createNumData(5))
                            .build()),
                    KV.of(
                        PRIMARY_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(PRIMARY_MINOR_KEY)
                            .putDataStore(PRIMARY_METRIC, CommonUtils.createNumData(10))
                            .build()))
                .withCoder(CommonUtils.getKvTSAccumCoder()));

    String outputName = options.getValueInBoundsOutputMetricName();

    PCollection<String> result =
        values
            .apply(new ValueInBoundsGFn())
            .apply(
                Filter.by(
                    x ->
                        x.getKey()
                            .getMinorKeyString()
                            .equals(PRIMARY_MINOR_KEY.getMinorKeyString())))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMinorKeyString()
                                + "::"
                                + outputName
                                + "::"
                                + x.getValue()
                                    .getDataStoreOrDefault(
                                        outputName, Data.newBuilder().setIntVal(999).build())
                                    .getIntVal()));

    PAssert.that(result).containsInAnyOrder("PRIMARY::OUTPUT::10");

    p.run();
  }

  @Test
  public void testNoUpperFound() throws Exception {

    ValueInBoundsGFn valueWithinBoundsGraphFn = new ValueInBoundsGFn();

    ValueInBoundsOptions options = testOptions(p);

    PCollection<KV<TSKey, TSAccum>> values =
        p.apply(
            Create.of(
                    KV.of(
                        LOWER_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(LOWER_MINOR_KEY)
                            .putDataStore(LOWER_METRIC, CommonUtils.createNumData(5))
                            .build()),
                    KV.of(
                        PRIMARY_MINOR_KEY,
                        TSAccum.newBuilder()
                            .setKey(PRIMARY_MINOR_KEY)
                            .putDataStore(PRIMARY_METRIC, CommonUtils.createNumData(10))
                            .build()))
                .withCoder(CommonUtils.getKvTSAccumCoder()));

    String outputName = options.getValueInBoundsOutputMetricName();

    PCollection<String> result =
        values
            .apply(new ValueInBoundsGFn())
            .apply(
                Filter.by(
                    x ->
                        x.getKey()
                            .getMinorKeyString()
                            .equals(PRIMARY_MINOR_KEY.getMinorKeyString())))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMinorKeyString()
                                + "::"
                                + outputName
                                + "::"
                                + x.getValue()
                                    .getDataStoreOrDefault(
                                        outputName, Data.newBuilder().setIntVal(999).build())
                                    .getIntVal()));

    PAssert.that(result).containsInAnyOrder("PRIMARY::OUTPUT::10");

    p.run();
  }
}
