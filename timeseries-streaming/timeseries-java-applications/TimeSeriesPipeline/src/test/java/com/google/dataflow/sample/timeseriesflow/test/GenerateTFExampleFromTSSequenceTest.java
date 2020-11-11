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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.FeaturesFromIterableAccumSequence;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

/** TODO switch whole TSAccum building tests to validatePropertyTests */
public class GenerateTFExampleFromTSSequenceTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  /* Simple test to check values are processed from TSAccumSequence -> TF.Example Correctly */
  public void testCreateTSExampleFromTSAccumSequence() {

    TSAccum first =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .putDataStore("feature_a", Data.newBuilder().setFloatVal(1F).build())
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.START))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.START), Durations.fromSeconds(5)))
            .build();

    TSAccum second =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .putDataStore("feature_a", Data.newBuilder().setFloatVal(3F).build())
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.PLUS_FIVE_SECS),
                    Durations.fromSeconds(5)))
            .build();
    TSAccum third =
        TSAccum.newBuilder()
            .setKey(TSDataTestUtils.KEY_A_A)
            .putDataStore("feature_a", Data.newBuilder().setFloatVal(2F).build())
            .setLowerWindowBoundary(Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS))
            .setUpperWindowBoundary(
                Timestamps.add(
                    Timestamps.fromMillis(TSDataTestUtils.PLUS_TEN_SECS), Durations.fromSeconds(5)))
            .build();

    PCollectionTuple examples =
        p.apply(
                Create.of(
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
                                    Durations.fromSeconds(15)))
                            .setDuration(Durations.fromSeconds(5))
                            .setCount(3)
                            .build())))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(new FeaturesFromIterableAccumSequence(3, true));

    PAssert.that(examples.get(FeaturesFromIterableAccumSequence.TIME_SERIES_EXAMPLES))
        .containsInAnyOrder(
            Example.newBuilder()
                .setFeatures(
                    Features.newBuilder()
                        .putFeature(
                            String.join(
                                "-",
                                TSDataTestUtils.KEY_A_A.getMajorKey(),
                                TSDataTestUtils.KEY_A_A.getMinorKeyString(),
                                "feature_a"),
                            Feature.newBuilder()
                                .setFloatList(
                                    FloatList.newBuilder()
                                        .addValue(1F)
                                        .addValue(3F)
                                        .addValue(2F)
                                        .build())
                                .build())
                        .putFeature(
                            "METADATA_SPAN_START_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder().addValue(TSDataTestUtils.START).build())
                                .build())
                        .putFeature(
                            "METADATA_SPAN_END_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(
                                            Instant.ofEpochMilli(TSDataTestUtils.PLUS_TEN_SECS)
                                                .plus(Duration.standardSeconds(5))
                                                .getMillis())
                                        .build())
                                .build())
                        .putFeature(
                            "__CONFIG_TIMESTEPS-3",
                            Feature.newBuilder()
                                .setInt64List(Int64List.newBuilder().addValue(1L).build())
                                .build()))
                .build());

    p.run();
  }
}
