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
import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwo;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwoFn;
import com.google.dataflow.sample.timeseriesflow.metrics.CTypeTwo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
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

  public TSFlowGraph genType2ComputationGraph(TSFlowGraph tsFlowGraph, PipelineOptions options) {

    // **************************************************************
    // Step 5: Technical analysis are generated from the Spans of data, for example RSI and
    // loaded into the {@link TSAccumSequence} objects
    // **************************************************************

    List<PCollection<KV<TSKey, TSAccum>>> type2computations = new ArrayList<>();

    for (Class<? extends BTypeTwoFn> compute :
        Optional.ofNullable(getGenerateComputations().getBasicType2Metrics())
            .orElse(ImmutableList.of())) {
      LOG.info("Adding Basic Type 2 Computations " + compute.getSimpleName());
      try {

        type2computations.add(
            tsFlowGraph
                .getType1AccumSeqComputations()
                .apply(
                    compute.getCanonicalName(),
                    BTypeTwo.builder()
                        .setMapping(compute.newInstance().getContextualFn(options))
                        .build()));
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.error(String.format("Error adding Type 2 Basic Metric %s. %s!", compute, e));
      }
    }

    // Attach Complex Type 2 computations

    List<Class<? extends CTypeTwo>> complexType2Metrics =
        Optional.ofNullable(getGenerateComputations().getComplexType2Metrics())
            .orElse(ImmutableList.of());

    List<String> metricExclusionList = new ArrayList<>();

    for (Class<? extends CTypeTwo> complexType2 : complexType2Metrics) {

      try {
        CTypeTwo cTypeTwo = complexType2.newInstance();
        PCollection<KV<TSKey, TSAccum>> collection =
            tsFlowGraph
                .getType1AccumSeqComputations()
                .apply(complexType2.getCanonicalName(), cTypeTwo);
        Preconditions.checkNotNull(
            collection,
            String.format(
                "Type 2 metric returned null for metrics %s",
                cTypeTwo.getClass().getCanonicalName()));
        type2computations.add(collection);
        metricExclusionList.addAll(
            Optional.ofNullable(cTypeTwo.excludeFromOutput()).orElse(ImmutableList.of()));

        LOG.info("Adding Complex Type 2 Computations " + complexType2.getSimpleName());
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.error("Unable to use {} due to {}", complexType2.getName(), e);
      }
    }

    TSFlowGraph.Builder tsFlowGraphWithType2 = tsFlowGraph.toBuilder();
    tsFlowGraphWithType2.setMetricsExcludeList(metricExclusionList);

    if (type2computations.size() > 0) {
      return tsFlowGraphWithType2
          .setType2Computations(PCollectionList.of(type2computations).apply(Flatten.pCollections()))
          .build();
    }

    return tsFlowGraphWithType2.build();
  }
}
