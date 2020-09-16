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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.FeaturesFromIterableAccumSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.GenerateMajorKeyWindowSnapshot;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

public class TSAccumIterableToTFExampleTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final TSAccum FIRST_ACCUM =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_A)
          .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(), CommonUtils.createNumData(TSDataTestUtils.START))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum SECOND_ACCUM =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_A)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_TEN_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(2F))
          .build();

  @Test
  /* Simple test to check TF Example output from TSIterable */
  public void testTSIterableAccumeToTFExample() {

    List<Iterable<TSAccumSequence>> sequence =
        ImmutableList.of(
            Lists.newArrayList(
                TSAccumSequence.newBuilder()
                    .setKey(TSDataTestUtils.KEY_A_A)
                    .addAccums(FIRST_ACCUM)
                    .addAccums(SECOND_ACCUM)
                    .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
                    .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
                    .setDuration(Durations.fromSeconds(10))
                    .setCount(2)
                    .build()));

    PCollectionTuple examples =
        p.apply(Create.of(sequence)).apply(new FeaturesFromIterableAccumSequence(2));

    PAssert.that(examples.get(FeaturesFromIterableAccumSequence.TIME_SERIES_EXAMPLES))
        .containsInAnyOrder(
            Example.newBuilder()
                .setFeatures(
                    Features.newBuilder()
                        .putFeature(
                            "Key-A-MKey-a-MAX",
                            Feature.newBuilder()
                                .setFloatList(FloatList.newBuilder().addValue(1F).addValue(2F))
                                .build())
                        .putFeature(
                            "Key-A-MKey-a-FIRST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.START)
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS))
                                .build())
                        .putFeature(
                            "Key-A-MKey-a-LAST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS)
                                        .addValue(TSDataTestUtils.PLUS_TEN_SECS))
                                .build())
                        .putFeature(
                            "METADATA_SPAN_START_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder().addValue(TSDataTestUtils.START))
                                .build())
                        .putFeature(
                            "METADATA_SPAN_END_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(
                                            Timestamps.toMillis(
                                                Timestamps.add(
                                                    Timestamps.fromMillis(TSDataTestUtils.START),
                                                    Durations.fromSeconds(10)))))
                                .build())
                        .putFeature(
                            "__CONFIG_TIMESTEPS-2",
                            Feature.newBuilder()
                                .setInt64List(Int64List.newBuilder().addValue(1L))
                                .build()))
                .build());

    p.run();
  }

  @Test
  /* Simple test to check TF Example output from SnapShot TSAccum */
  public void testTSAccumSequenceToTFExample() {
    KV<TSKey, TSAccumSequence> sequence =
        KV.of(
            TSDataTestUtils.KEY_A_A,
            TSAccumSequence.newBuilder()
                .setKey(TSDataTestUtils.KEY_A_A)
                .addAccums(FIRST_ACCUM)
                .addAccums(SECOND_ACCUM)
                .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
                .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
                .setDuration(Durations.fromSeconds(10))
                .setCount(2)
                .build());

    PCollectionTuple examples =
        p.apply(Create.of(sequence))
            .apply(GenerateMajorKeyWindowSnapshot.generateWindowSnapshot())
            .apply(new FeaturesFromIterableAccumSequence(2));

    PAssert.that(examples.get(FeaturesFromIterableAccumSequence.TIME_SERIES_EXAMPLES))
        .containsInAnyOrder(
            Example.newBuilder()
                .setFeatures(
                    Features.newBuilder()
                        .putFeature(
                            "Key-A-MKey-a-MAX",
                            Feature.newBuilder()
                                .setFloatList(FloatList.newBuilder().addValue(1F).addValue(2F))
                                .build())
                        .putFeature(
                            "Key-A-MKey-a-FIRST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.START)
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS))
                                .build())
                        .putFeature(
                            "Key-A-MKey-a-LAST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS)
                                        .addValue(TSDataTestUtils.PLUS_TEN_SECS))
                                .build())
                        .putFeature(
                            "METADATA_SPAN_START_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder().addValue(TSDataTestUtils.START))
                                .build())
                        .putFeature(
                            "METADATA_SPAN_END_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(
                                            Timestamps.toMillis(
                                                Timestamps.add(
                                                    Timestamps.fromMillis(TSDataTestUtils.START),
                                                    Durations.fromSeconds(10)))))
                                .build())
                        .putFeature(
                            "__CONFIG_TIMESTEPS-2",
                            Feature.newBuilder()
                                .setInt64List(Int64List.newBuilder().addValue(1L))
                                .build()))
                .build());

    p.run();
  }

  @Test
  /* Simple test to check TF Example output from SnapShot TSAccum */
  public void testTSIterableAccumeToTFExampleTwoKeys() {
    KV<TSKey, TSAccumSequence> sequenceA =
        KV.of(
            TSDataTestUtils.KEY_A_A,
            TSAccumSequence.newBuilder()
                .setKey(TSDataTestUtils.KEY_A_A)
                .addAccums(FIRST_ACCUM)
                .addAccums(SECOND_ACCUM)
                .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
                .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
                .setDuration(Durations.fromSeconds(10))
                .setCount(2)
                .build());

    KV<TSKey, TSAccumSequence> sequenceB =
        KV.of(
            TSDataTestUtils.KEY_B_A,
            TSAccumSequence.newBuilder()
                .setKey(TSDataTestUtils.KEY_B_A)
                .addAccums(FIRST_ACCUM)
                .addAccums(SECOND_ACCUM)
                .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
                .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
                .setDuration(Durations.fromSeconds(10))
                .setCount(2)
                .build());

    PCollectionTuple examples =
        p.apply(Create.of(sequenceA, sequenceB))
            .apply(GenerateMajorKeyWindowSnapshot.generateWindowSnapshot())
            .apply(new FeaturesFromIterableAccumSequence(2));

    PAssert.that(examples.get(FeaturesFromIterableAccumSequence.TIME_SERIES_EXAMPLES))
        .containsInAnyOrder(
            Example.newBuilder()
                .setFeatures(
                    Features.newBuilder()
                        .putFeature(
                            "Key-A-MKey-a-MAX",
                            Feature.newBuilder()
                                .setFloatList(FloatList.newBuilder().addValue(1F).addValue(2F))
                                .build())
                        .putFeature(
                            "Key-A-MKey-a-FIRST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.START)
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS))
                                .build())
                        .putFeature(
                            "Key-A-MKey-a-LAST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS)
                                        .addValue(TSDataTestUtils.PLUS_TEN_SECS))
                                .build())
                        .putFeature(
                            "Key-B-MKey-a-MAX",
                            Feature.newBuilder()
                                .setFloatList(FloatList.newBuilder().addValue(1F).addValue(2F))
                                .build())
                        .putFeature(
                            "Key-B-MKey-a-FIRST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.START)
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS))
                                .build())
                        .putFeature(
                            "Key-B-MKey-a-LAST_TIMESTAMP",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(TSDataTestUtils.PLUS_FIVE_SECS)
                                        .addValue(TSDataTestUtils.PLUS_TEN_SECS))
                                .build())
                        .putFeature(
                            "METADATA_SPAN_START_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder().addValue(TSDataTestUtils.START))
                                .build())
                        .putFeature(
                            "METADATA_SPAN_END_TS",
                            Feature.newBuilder()
                                .setInt64List(
                                    Int64List.newBuilder()
                                        .addValue(
                                            Timestamps.toMillis(
                                                Timestamps.add(
                                                    Timestamps.fromMillis(TSDataTestUtils.START),
                                                    Durations.fromSeconds(10)))))
                                .build())
                        .putFeature(
                            "__CONFIG_TIMESTEPS-2",
                            Feature.newBuilder()
                                .setInt64List(Int64List.newBuilder().addValue(1L))
                                .build()))
                .build());

    p.run();
  }
}
