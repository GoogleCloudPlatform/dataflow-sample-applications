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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSBaseCombiner;
import com.google.dataflow.sample.timeseriesflow.metrics.BType2;
import com.google.dataflow.sample.timeseriesflow.metrics.BType2Fn;
import com.google.dataflow.sample.timeseriesflow.transforms.AddWindowBoundaryToTSAccum;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.CreateCompositeTSAccum;
import com.google.dataflow.sample.timeseriesflow.transforms.MergeAllTypeCompsInSameKeyWindow;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation.ComputeType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class GraphType2Comp implements Serializable {

  public static final Logger LOG = LoggerFactory.getLogger(GraphType2Comp.class);

  public abstract GenerateComputations getGenerateComputations();

  public static GraphType2Comp create(GenerateComputations generateComputations) {
    return builder().setGenerateComputations(generateComputations).build();
  }

  public static Builder builder() {
    return new AutoValue_GraphType2Comp.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setGenerateComputations(GenerateComputations newGenerateComputations);

    public abstract GraphType2Comp build();
  }

  public PCollection<KV<TSKey, TSAccum>> genType2ComputationGraph(
      PCollectionTuple allDataTypes,
      PCollection<KV<TSKey, TSAccum>> type1Computations,
      PipelineOptions options) {

    // **************************************************************
    // Step 5: Technical analysis are generated from the Spans of data, for example RSI and
    // loaded into the {@link TSAccumSequence} objects
    // **************************************************************

    List<PCollection<KV<TSKey, TSAccum>>> type2computations = new ArrayList<>();

    List<PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>>
        singleKeyComp = new ArrayList<>();

    for (Class<? extends BType2Fn> compute :
        Optional.ofNullable(getGenerateComputations().basicType2Metrics())
            .orElse(ImmutableList.of())) {
      LOG.info("Adding Basic Type 2 Computations " + compute.getSimpleName());
      try {
        singleKeyComp.add(
            BType2.builder().setMapping(compute.newInstance().getContextualFn(options)).build());
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.error(String.format("Error adding Type 2 Basic Metric %s. %s!", compute, e));
      }
    }

    List<PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>>
        type2NumericComputations =
            Optional.ofNullable(getGenerateComputations().type2NumericComputations())
                .orElse(ImmutableList.of());

    List<PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>>
        compKeyComp = new ArrayList<>();

    for (PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>
        compute : type2NumericComputations) {

      TypeTwoComputation typeTwoComputation =
          compute.getClass().getAnnotation(TypeTwoComputation.class);

      if (typeTwoComputation.computeType().equals(ComputeType.SINGLE_KEY)) {
        LOG.info("Adding Type 2 Computation For Single Key " + compute.getClass());
        singleKeyComp.add(compute);
      }

      if (typeTwoComputation.computeType().equals(ComputeType.COMPOSITE_KEY)) {
        LOG.info("Adding Type 2 Computation For Composite Key " + compute.getClass());
        compKeyComp.add(compute);
      }
    }

    if (singleKeyComp.size() > 0) {

      PCollection<KV<TSKey, TSAccumSequence>> sequencedAccums =
          type1Computations.apply(
              ConvertAccumToSequence.builder()
                  .setWindow(
                      Window.into(
                          SlidingWindows.of(getGenerateComputations().type2SlidingWindowDuration())
                              .every(getGenerateComputations().type1FixedWindow())))
                  .build());

      singleKeyComp.forEach(x -> type2computations.add(sequencedAccums.apply(x)));
    }

    if (compKeyComp.size() > 0) {

      // --------------- Apply key merge if any, this produces a TSAccum which has values
      // from multiple streams
      List<PCollection<KV<TSKey, TSAccum>>> keyMergeList = new ArrayList<>();

      if (getGenerateComputations().type1KeyMerge() != null
          && getGenerateComputations().type1KeyMerge().size() > 0) {

        for (CreateCompositeTSAccum transform : getGenerateComputations().type1KeyMerge()) {
          keyMergeList.add(type1Computations.apply(transform));
        }
      }

      if (keyMergeList.size() > 0) {
        PCollection<KV<TSKey, TSAccumSequence>> sequencedAccumsCompKey =
            PCollectionList.of(keyMergeList)
                .apply(Flatten.pCollections())
                .apply(
                    ConvertAccumToSequence.builder()
                        .setWindow(
                            Window.into(
                                SlidingWindows.of(
                                        getGenerateComputations().type2SlidingWindowDuration())
                                    .every(getGenerateComputations().type1FixedWindow())))
                        .build());

        compKeyComp.forEach(
            x ->
                type2computations.add(
                    sequencedAccumsCompKey
                        .apply(x)
                        .apply("CompKeyReify", Reify.windowsInValue())
                        .apply("CompKeyAddWin", ParDo.of(new AddWindowBoundaryToTSAccum()))
                        .apply("SetInternalState", ParDo.of(new SetInternalState()))));
      }
    }

    // --------------- Merge Type 1 & Type 2 aggregations

    PCollectionList<KV<TSKey, TSAccum>> type1AndType2Computations =
        PCollectionList.of(type1Computations).and(type2computations);

    PCollection<KV<TSKey, TSAccum>> mergedComputations =
        type1AndType2Computations.apply(
            MergeAllTypeCompsInSameKeyWindow.withMergeWindow(
                Window.into(FixedWindows.of(getGenerateComputations().type1FixedWindow()))));

    return mergedComputations;
  }
  /** */
  private static class SetInternalState extends DoFn<KV<TSKey, TSAccum>, KV<TSKey, TSAccum>> {

    @ProcessElement
    public void process(@Element KV<TSKey, TSAccum> element, OutputReceiver<KV<TSKey, TSAccum>> o) {

      o.output(
          KV.of(
              element.getKey(),
              element
                  .getValue()
                  .toBuilder()
                  .putMetadata(TSBaseCombiner._BASE_COMBINER, "t")
                  .build()));
    }
  }
}
