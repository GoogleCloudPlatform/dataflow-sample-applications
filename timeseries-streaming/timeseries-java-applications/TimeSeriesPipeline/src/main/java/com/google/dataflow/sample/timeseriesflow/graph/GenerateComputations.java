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
package com.google.dataflow.sample.timeseriesflow.graph;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSBaseCombiner;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TupleTypes;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwoFn;
import com.google.dataflow.sample.timeseriesflow.metrics.CTypeTwo;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.CreateCompositeTSAccum;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.MergeAllTypeCompsInSameKeyWindow;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import com.google.dataflow.sample.timeseriesflow.verifier.TSDataPointVerifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
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

  public abstract Boolean getEnableGapFill();

  public abstract Duration getType1FixedWindow();

  public @Nullable abstract Duration getType2SlidingWindowDuration();

  public @Nullable abstract Integer getHotKeyFanOut();

  public @Nullable abstract List<CreateCompositeTSAccum> getType1KeyMerge();

  public @Nullable abstract List<Class<? extends BTypeOne>> getBasicType1Metrics();

  public @Nullable abstract List<Class<? extends BTypeTwoFn>> getBasicType2Metrics();

  public @Nullable abstract List<Class<? extends CTypeTwo>> getComplexType2Metrics();

  public @Nullable abstract Class<
          ? extends PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccum>>>>
      getPostProcessRules();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GenerateComputations.Builder();
  };

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setEnableGapFill(Boolean value);

    public abstract Builder setType1FixedWindow(Duration value);

    public abstract Builder setType2SlidingWindowDuration(Duration value);

    public abstract Builder setBasicType1Metrics(List<Class<? extends BTypeOne>> bTypeOne);

    public abstract Builder setBasicType2Metrics(List<Class<? extends BTypeTwoFn>> bTypeTwoFns);

    public abstract Builder setHotKeyFanOut(Integer value);

    public abstract Builder setType1KeyMerge(List<CreateCompositeTSAccum> value);

    public @Nullable abstract Builder setComplexType2Metrics(
        List<Class<? extends CTypeTwo>> complexType2Metrics);

    public @Nullable abstract Builder setPostProcessRules(
        Class<
                ? extends
                    PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccum>>>>
            postProcessRules);

    public abstract GenerateComputations build();
  }

  public static GenerateComputations.Builder fromPiplineOptions(TSFlowOptions options) {
    Builder builder = new AutoValue_GenerateComputations.Builder();

    Preconditions.checkArgument(
        options.getTypeOneComputationsLengthInSecs() != null
            && options.getTypeTwoComputationsLengthInSecs() != null,
        "Both type 1 and type 2 durations must be set");

    builder
        .setType1FixedWindow(Duration.standardSeconds(options.getTypeOneComputationsLengthInSecs()))
        .setType2SlidingWindowDuration(
            Duration.standardSeconds(options.getTypeTwoComputationsLengthInSecs()));

    // Check if Gap fill is enabled, if yes add it to computation.
    if (options.isGapFillEnabled() != null && options.isGapFillEnabled()) {
      Preconditions.checkArgument(
          options.getAbsoluteStopTimeMSTimestamp() != null || options.getTTLDurationSecs() != null,
          "If Gap fill is enabled then either TTL or absolute stop time must be set.");
      builder.setEnableGapFill(true);
    } else {
      builder.setEnableGapFill(false);
    }

    // Check for Type 1 Basic Metrics
    if (options.getTypeOneBasicMetrics() != null) {
      builder.setBasicType1Metrics(
          GenerateComputations.getClasses(options.getTypeOneBasicMetrics()));
    }

    // Check for Type 2 Basic Metrics
    if (options.getTypeTwoBasicMetrics() != null) {
      builder.setBasicType2Metrics(
          GenerateComputations.getClasses(options.getTypeTwoBasicMetrics()));
    }

    // Check for Type 2 Complex Metrics
    if (options.getTypeTwoComplexMetrics() != null) {
      builder.setComplexType2Metrics(
          GenerateComputations.getClasses(options.getTypeTwoComplexMetrics()));
    }

    return builder;
  }

  private static <T> List<Class<? extends T>> getClasses(List<String> clazzList) {

    List<Class<? extends T>> classes = new ArrayList<>();

    // Check for Type 2 Basic Metrics
    for (String metric : clazzList) {
      String clazzName =
          String.format("com.google.dataflow.sample.timeseriesflow.metrics.core.%s", metric);
      LOG.info("Finding class : {}", clazzName);
      try {
        classes.add((Class<T>) Class.forName(clazzName));

      } catch (ClassNotFoundException ex) {
        LOG.error("Class not found : {} is this metric class available in the classpath?", metric);
        throw new IllegalStateException("Unable to find metric.");
      } catch (ClassCastException ex) {
        LOG.error("Class {} does not extend {}", metric, new TypeDescriptor<T>() {}.getClass());
        throw new IllegalStateException("Metric class setup incorrectly.");
      }
    }

    return classes;
  }

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<TSDataPoint> input) {

    TSFlowGraph.Builder tsFlowGraph = TSFlowGraph.builder();

    // Attach pipeline options for use with graph attach components
    tsFlowGraph.setPipelineOptions(input.getPipeline().getOptions());

    // Run verification logic against the TSDataPoint
    PCollection<KV<TSKey, TSDataPoint>> verifiedTSDataPoints =
        input
            .apply(TSDataPointVerifier.create())
            .apply("AddDataPointKeys", WithKeys.of(TSDataPoint::getKey))
            .setCoder(CommonUtils.getKvTSDataPointCoder());

    // Run any enrichment activity, for example Gap Filling if enabled
    PCollection<KV<TSKey, TSDataPoint>> enrichedTSDataPoints = null;

    // If GapFilling is enabled then fill gaps using PerfectRectangles config
    if (this.getEnableGapFill()) {
      enrichedTSDataPoints =
          verifiedTSDataPoints.apply(
              PerfectRectangles.fromPipelineOptions(
                  input.getPipeline().getOptions().as(TSFlowOptions.class)));
    } else {
      enrichedTSDataPoints = verifiedTSDataPoints;
    }

    tsFlowGraph.setEnrichedTSDataPoints(enrichedTSDataPoints);

    // Filter the data types into tuples for combiners that match the data type

    // TODO Adding and removing the KV is inefficient, change signatures to just assume KV

    PCollectionTuple allDataTypes =
        enrichedTSDataPoints
            .apply(Values.create())
            .apply(Window.into(FixedWindows.of(getType1FixedWindow())))
            .apply(new FilterByDataType());

    tsFlowGraph.setDatabyType(allDataTypes);

    // --------------- Compute Type 1 aggregations

    // -- Obtain any type 1 aggregations requested by complex type 2

    PCollection<KV<TSKey, TSAccum>> type1Computations =
        GraphType1Comp.create(this).genType1ComputationGraph(allDataTypes);

    tsFlowGraph.setType1Computations(type1Computations);

    // Generate the TSAccumSequence if we have any type 2 computations that require it
    if (Optional.ofNullable(getBasicType2Metrics()).orElse(ImmutableList.of()).size() > 0
        || Optional.ofNullable(getComplexType2Metrics()).orElse(ImmutableList.of()).size() > 0) {
      tsFlowGraph.setType1AccumSeqComputations(
          type1Computations.apply(
              ConvertAccumToSequence.builder()
                  .setWindow(
                      Window.into(
                          SlidingWindows.of(getType2SlidingWindowDuration())
                              .every(getType1FixedWindow())))
                  .build()));
    }

    // --------------- Compute Type 2 aggregations

    // **************************************************************
    // Step 5: Technical analysis are generated from the Spans of data, for example RSI and
    // loaded into the {@link TSAccumSequence} objects
    // **************************************************************

    TSFlowGraph finalGraph =
        GraphType2Comp.create(this)
            .genType2ComputationGraph(tsFlowGraph.build(), input.getPipeline().getOptions());

    // --------------- Merge Type 1 & Type 2 aggregations
    PCollectionList<KV<TSKey, TSAccum>> allComputations =
        PCollectionList.of(
            finalGraph
                .getType1Computations()
                .apply(
                    "WindowT1",
                    Window.<KV<TSKey, TSAccum>>into(FixedWindows.of(getType1FixedWindow()))));

    // Type 2 is not a hard requirement
    if (finalGraph.getType2Computations() != null) {
      allComputations =
          allComputations.and(
              finalGraph
                  .getType2Computations()
                  .apply(
                      "WindowT2",
                      Window.<KV<TSKey, TSAccum>>into(FixedWindows.of(getType1FixedWindow()))));
    }

    PCollection<KV<TSKey, TSAccum>> metrics =
        allComputations.apply("FlattenT1T2", Flatten.pCollections());

    PCollection<KV<TSKey, TSAccum>> output =
        metrics.apply(
            MergeAllTypeCompsInSameKeyWindow.create()
                .builder()
                .setMetricExcludeList(finalGraph.getMetricsExcludeList())
                .build());

    if (getPostProcessRules() != null) {
      try {
        output = output.apply(getPostProcessRules().newInstance());
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.error("Enable to add Rule", e);
      }
    }

    return output.apply(ParDo.of(new ClearInternalState()));
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
                      ImmutableList.of(
                          TupleTypes.t_double,
                          TupleTypes.t_long,
                          TupleTypes.t_str,
                          TupleTypes.t_float))));
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
          case FLOAT_VAL:
            {
              mo.get(TupleTypes.t_float).output(data);
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

  /** */
  private static class ClearInternalState extends DoFn<KV<TSKey, TSAccum>, KV<TSKey, TSAccum>> {

    @ProcessElement
    public void process(@Element KV<TSKey, TSAccum> element, OutputReceiver<KV<TSKey, TSAccum>> o) {

      o.output(
          KV.of(
              element.getKey(),
              element
                  .getValue()
                  .toBuilder()
                  .removeMetadata(TSBaseCombiner._BASE_COMBINER)
                  .build()));
    }
  }
}
