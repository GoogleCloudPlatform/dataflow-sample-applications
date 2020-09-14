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
package com.google.dataflow.sample.timeseriesflow.transforms;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.TimeseriesStreamingOptions;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TupleTypes;
import com.google.dataflow.sample.timeseriesflow.verifier.TSDataPointVerifier;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts each property of the stream of {@link KV < TSKey ,TSDataPoint>} values.
 *
 * <p>The Type 1 Fixed window size dictates the type 2 offset slide.
 *
 * <p>Step 1: Type 1 computations are created for each of the properties and stored in a {@link
 * TSAccum}. Using the Type1WindowSize.
 *
 * <p>Step 2: {@link TSAccumSequence} are generated from the {@link TSAccum} objects using the
 * Type2WindowSize.
 *
 * <p>Step 3: Technical analysis are generated from the Spans of data, for example RSI and loaded
 * into the {@link TSAccumSequence} objects
 *
 * <p>Step 4: All properties are collapsed back together into a {@link KV< TSKey
 * ,Iterable<TSAccumSequence>}
 *
 * <p>Step 5: Type 1 and Type 2 results are merged back together {@link
 * KV<TSKey,Iterable<TSAccumSequence>}
 */
@AutoValue
@Experimental
public abstract class GenerateComputations
    extends PTransform<PCollection<TSDataPoint>, PCollection<KV<TSKey, TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(GenerateComputations.class);

  public abstract Duration type1FixedWindow();

  public @Nullable abstract Duration type2SlidingWindowDuration();

  @Experimental
  public @Nullable abstract PerfectRectangles perfectRectangles();

  public @Nullable abstract Integer hotKeyFanOut();

  abstract List<CombineFn<TSDataPoint, TSAccum, TSAccum>> type1NumericComputations();

  public @Nullable abstract List<
          PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>>
      type2NumericComputations();

  public @Nullable abstract List<PTransform<PCollection<TSDataPoint>, PCollection<TSAccum>>>
      type1CategoricalComputations();

  public @Nullable abstract List<
          PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccum>>>>
      type2CategoricalComputations();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GenerateComputations.Builder();
  };

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setType1FixedWindow(Duration value);

    public abstract Builder setType2SlidingWindowDuration(Duration value);

    public abstract Builder setPerfectRectangles(PerfectRectangles perfectRectangles);

    public abstract Builder setHotKeyFanOut(Integer value);

    public abstract Builder setType1NumericComputations(
        List<CombineFn<TSDataPoint, TSAccum, TSAccum>> value);

    public @Nullable abstract Builder setType2NumericComputations(
        List<PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>>
            value);

    public abstract Builder setType1CategoricalComputations(
        List<PTransform<PCollection<TSDataPoint>, PCollection<TSAccum>>> value);

    public abstract Builder setType2CategoricalComputations(
        List<PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccum>>>> value);

    public abstract GenerateComputations build();
  }

  public GenerateComputations withPerfectRectangles(PerfectRectangles perfectRectangles) {
    return this.toBuilder().setPerfectRectangles(perfectRectangles).build();
  }

  public static GenerateComputations.Builder fromPiplineOptions(
      TimeseriesStreamingOptions options) {
    Preconditions.checkArgument(
        options.getTypeOneComputationsLengthInSecs() != null
            && options.getTypeTwoComputationsLengthInSecs() != null,
        "Both type 1 and type 2 durations must be set");

    return new AutoValue_GenerateComputations.Builder()
        .setType1FixedWindow(Duration.standardSeconds(options.getTypeOneComputationsLengthInSecs()))
        .setType2SlidingWindowDuration(
            Duration.standardSeconds(options.getTypeTwoComputationsLengthInSecs()));
  }

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<TSDataPoint> input) {

    // Run verification logic against the TSDataPoint
    PCollection<KV<TSKey, TSDataPoint>> verifiedTSDataPoints =
        input
            .apply(new TSDataPointVerifier())
            .apply("AddDataPointKeys", WithKeys.of(TSDataPoint::getKey))
            .setCoder(CommonUtils.getKvTSDataPointCoder());

    // Run any enrichment activity, for example Gap Filling if enabled
    PCollection<KV<TSKey, TSDataPoint>> enrichedTSDataPoints = null;

    // If GapFilling is enabled then fill gaps using PerfectRectangles config
    if (this.perfectRectangles() != null) {
      enrichedTSDataPoints = verifiedTSDataPoints.apply(perfectRectangles());
    } else {
      enrichedTSDataPoints = verifiedTSDataPoints;
    }

    // Filter the data types into tuples for combiners that match the data type

    // TODO Adding and removing the KV is inefficient, change signatures to just assume KV

    PCollectionTuple allDataTypes =
        enrichedTSDataPoints
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(type1FixedWindow())))
            .apply(new FilterByDataType());

    // --------------- Compute Type 1 aggregations

    List<PCollection<KV<TSKey, TSAccum>>> coll = new ArrayList<>();

    int hotKeyfanout = Optional.ofNullable(hotKeyFanOut()).orElse(5);

    for (CombineFn<TSDataPoint, TSAccum, TSAccum> combine : type1NumericComputations()) {
      // TODO add other types
      coll.add(
          allDataTypes
              .get(TupleTypes.t_double)
              .apply("Double Types", WithKeys.of(x -> TSKey.newBuilder(x.getKey()).build()))
              .setCoder(CommonUtils.getKvTSDataPointCoder())
              .apply(
                  "Combine Doubles",
                  Combine.<TSKey, TSDataPoint, TSAccum>perKey(combine)
                      .withHotKeyFanout(hotKeyfanout)));
      coll.add(
          allDataTypes
              .get(TupleTypes.t_int)
              .apply("Integer Types", WithKeys.of(x -> TSKey.newBuilder(x.getKey()).build()))
              .setCoder(CommonUtils.getKvTSDataPointCoder())
              .apply(
                  "Combine Integers",
                  Combine.<TSKey, TSDataPoint, TSAccum>perKey(combine)
                      .withHotKeyFanout(hotKeyfanout)));
      coll.add(
          allDataTypes
              .get(TupleTypes.t_long)
              .apply("Long Types", WithKeys.of(x -> TSKey.newBuilder(x.getKey()).build()))
              .setCoder(CommonUtils.getKvTSDataPointCoder())
              .apply(
                  "Combine Longs",
                  Combine.<TSKey, TSDataPoint, TSAccum>perKey(combine)
                      .withHotKeyFanout(hotKeyfanout)));
    }

    PCollectionList<KV<TSKey, TSAccum>> allNumericAccums = PCollectionList.of(coll);

    PCollection<KV<TSKey, TSAccum>> type1Computations =
        allNumericAccums
            .apply("Flatten For WindowData", Flatten.pCollections())
            .setCoder(CommonUtils.getKvTSAccumCoder())
            .apply(
                ParDo.of(
                    new DoFn<KV<TSKey, TSAccum>, KV<TSKey, TSAccum>>() {
                      @ProcessElement
                      public void process(ProcessContext pc) {
                        // TODO confirm this issue is no longer a concern.
                        if (pc.pane().isUnknown()) {
                          LOG.error("Unknown Pane! This is a bug. " + pc.element());
                        }
                        pc.output(pc.element());
                      }
                    }))
            .apply(Reify.windowsInValue())
            .apply(ParDo.of(new AddWindowBoundaryToTSAccum()));

    // --------------- Compute Type 2 aggregations

    // **************************************************************
    // Step 5: Technical analysis are generated from the Spans of data, for example RSI and
    // loaded into the {@link TSAccumSequence} objects
    // **************************************************************

    if (type2NumericComputations() != null) {

      List<PCollection<KV<TSKey, TSAccum>>> type2computations = new ArrayList<>();

      for (PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>
          compute : type2NumericComputations()) {

        type2computations.add(
            type1Computations
                .apply(
                    ConvertAccumToSequence.builder()
                        .setWindow(
                            Window.into(
                                SlidingWindows.of(type2SlidingWindowDuration())
                                    .every(type1FixedWindow())))
                        .build())
                .apply(compute));
      }

      // --------------- Merge Type 1 & Type 2 aggregations

      PCollectionList<KV<TSKey, TSAccum>> type1AndType2Computations =
          PCollectionList.of(type1Computations).and(type2computations);

      PCollection<KV<TSKey, TSAccum>> mergedComputations =
          type1AndType2Computations.apply(
              MergeAllTypeCompsInSameKeyWindow.withMergeWindow(
                  Window.into(FixedWindows.of(type1FixedWindow()))));

      return mergedComputations;
    }
    return type1Computations;
  }

  private static class AddWindowBoundaryToTSAccum
      extends DoFn<KV<TSKey, ValueInSingleWindow<TSAccum>>, KV<TSKey, TSAccum>> {

    @ProcessElement
    public void process(
        @Element KV<TSKey, ValueInSingleWindow<TSAccum>> input,
        OutputReceiver<KV<TSKey, TSAccum>> o) {

      TSAccum.Builder tsAccum = input.getValue().getValue().toBuilder();

      if (input.getValue().getWindow() instanceof IntervalWindow) {
        IntervalWindow intervalWindow = (IntervalWindow) input.getValue().getWindow();
        tsAccum
            .setUpperWindowBoundary(Timestamps.fromMillis(intervalWindow.end().getMillis()))
            .setLowerWindowBoundary(Timestamps.fromMillis(intervalWindow.start().getMillis()));
      } else {
        LOG.error(" Bounded window detected instead of Interval Window, this is a bug!");
      }

      o.output(KV.of(input.getKey(), tsAccum.build()));
    }
  }

  private static class FilterByDataType
      extends PTransform<PCollection<TSDataPoint>, PCollectionTuple> {

    @Override
    public PCollectionTuple expand(PCollection<TSDataPoint> input) {
      return input.apply(
          ParDo.of(new TypeFilter())
              .withOutputTags(
                  TupleTypes.t_int,
                  TupleTagList.of(
                      ImmutableList.of(TupleTypes.t_double, TupleTypes.t_long, TupleTypes.t_str))));
    }

    /** Takes a {@link TSDataPoint} and outputs collection based on {@link TupleTypes} */
    private static class TypeFilter extends DoFn<TSDataPoint, TSDataPoint> {

      @ProcessElement
      public void process(@Element TSDataPoint data, MultiOutputReceiver mo) {
        switch (data.getData().getDataPointCase()) {
          case INT_VAL:
            {
              mo.get(TupleTypes.t_int).output(data);
              return;
            }
          case DOUBLE_VAL:
            {
              mo.get(TupleTypes.t_double).output(data);
              return;
            }
          case LONG_VAL:
            {
              mo.get(TupleTypes.t_long).output(data);
              return;
            }
          case CATEGORICAL_VAL:
            {
              mo.get(TupleTypes.t_str).output(data);
              return;
            }
          case DATAPOINT_NOT_SET:
            {
              throw new IllegalStateException(
                  String.format(
                      "At least one Data type needs to be set for Data with Key %s at timestamp %s",
                      data.getKey(), data.getTimestamp()));
            }
        }
      }
    }
  }
}
