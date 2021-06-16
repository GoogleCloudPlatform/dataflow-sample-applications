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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSNumericCombiner;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TupleTypes;
import com.google.dataflow.sample.timeseriesflow.transforms.AddWindowBoundaryToTSAccum;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class GraphType1Comp implements Serializable {

  public static final Logger LOG = LoggerFactory.getLogger(GraphType1Comp.class);

  public abstract GenerateComputations getGenerateComputations();

  public static GraphType1Comp create(GenerateComputations generateComputations) {
    return builder().setGenerateComputations(generateComputations).build();
  }

  public static Builder builder() {
    return new AutoValue_GraphType1Comp.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setGenerateComputations(GenerateComputations newGenerateComputations);

    public abstract GraphType1Comp build();
  }

  public PCollection<KV<TSKey, TSAccum>> genType1ComputationGraph(PCollectionTuple allDataTypes) {

    // --------------- Compute Type 1 aggregations

    List<PCollection<KV<TSKey, TSAccum>>> coll = new ArrayList<>();

    // Check if the Type 1 computations have been set by the user if not then set the base type 1
    // computations

    List<Class<? extends BTypeOne>> basicTypeOne =
        new ArrayList<>(
            Optional.ofNullable(getGenerateComputations().getBasicType1Metrics())
                .orElse(ImmutableList.of()));

    Optional.ofNullable(getGenerateComputations().getComplexType2Metrics())
        .orElse(ImmutableList.of())
        .forEach(
            x -> {
              try {
                basicTypeOne.addAll(
                    Optional.ofNullable(x.newInstance().requiredTypeOne())
                        .orElse(ImmutableList.of()));
              } catch (InstantiationException | IllegalAccessException e) {
                LOG.error("Unable to add required type one metric", e);
              }
            });

    basicTypeOne.forEach(x -> LOG.info("Adding Basic Type 1 computation : {}", x));

    List<CombineFn<TSDataPoint, TSAccum, TSAccum>> type1 =
        ImmutableList.of(new TSNumericCombiner(basicTypeOne));

    int hotKeyfanout = Optional.ofNullable(getGenerateComputations().getHotKeyFanOut()).orElse(5);

    coll.addAll(addTypeOneCombiner(allDataTypes, hotKeyfanout, type1));

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
            .apply("Type1Reify", Reify.windowsInValue())
            .apply("Type1AddWin", ParDo.of(new AddWindowBoundaryToTSAccum()));

    return type1Computations;
  }

  public static List<PCollection<KV<TSKey, TSAccum>>> addTypeOneCombiner(
      PCollectionTuple allDataTypes,
      int hotKeyfanout,
      List<CombineFn<TSDataPoint, TSAccum, TSAccum>> combines) {
    List<PCollection<KV<TSKey, TSAccum>>> coll = new ArrayList<>();

    for (CombineFn<TSDataPoint, TSAccum, TSAccum> comb : combines) {
      coll.addAll(addTypeOneCombiner(allDataTypes, hotKeyfanout, comb));
    }

    return coll;
  }

  public static List<PCollection<KV<TSKey, TSAccum>>> addTypeOneCombiner(
      PCollectionTuple allDataTypes,
      int hotKeyfanout,
      CombineFn<TSDataPoint, TSAccum, TSAccum> combine) {

    List<PCollection<KV<TSKey, TSAccum>>> coll = new ArrayList<>();

    coll.add(
        allDataTypes
            .get(TupleTypes.t_double)
            .apply("Double Types", WithKeys.of(x -> TSKey.newBuilder(x.getKey()).build()))
            .setCoder(CommonUtils.getKvTSDataPointCoder())
            .apply(
                String.format("Doubles-%s", combine.getClass().getCanonicalName()),
                Combine.<TSKey, TSDataPoint, TSAccum>perKey(combine)
                    .withHotKeyFanout(hotKeyfanout)));
    coll.add(
        allDataTypes
            .get(TupleTypes.t_int)
            .apply("Integer Types", WithKeys.of(x -> TSKey.newBuilder(x.getKey()).build()))
            .setCoder(CommonUtils.getKvTSDataPointCoder())
            .apply(
                String.format("Integers-%s", combine.getClass().getCanonicalName()),
                Combine.<TSKey, TSDataPoint, TSAccum>perKey(combine)
                    .withHotKeyFanout(hotKeyfanout)));
    coll.add(
        allDataTypes
            .get(TupleTypes.t_long)
            .apply("Long Types", WithKeys.of(x -> TSKey.newBuilder(x.getKey()).build()))
            .setCoder(CommonUtils.getKvTSDataPointCoder())
            .apply(
                String.format("Longs-%s", combine.getClass().getCanonicalName()),
                Combine.<TSKey, TSDataPoint, TSAccum>perKey(combine)
                    .withHotKeyFanout(hotKeyfanout)));
    coll.add(
        allDataTypes
            .get(TupleTypes.t_float)
            .apply("Float Types", WithKeys.of(x -> TSKey.newBuilder(x.getKey()).build()))
            .setCoder(CommonUtils.getKvTSDataPointCoder())
            .apply(
                String.format("Float-%s", combine.getClass().getCanonicalName()),
                Combine.<TSKey, TSDataPoint, TSAccum>perKey(combine)
                    .withHotKeyFanout(hotKeyfanout)));
    return coll;
  }
}
