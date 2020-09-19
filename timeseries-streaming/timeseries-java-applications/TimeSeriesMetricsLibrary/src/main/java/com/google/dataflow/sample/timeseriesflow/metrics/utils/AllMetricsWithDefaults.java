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
package com.google.dataflow.sample.timeseriesflow.metrics.utils;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSNumericCombiner;
import com.google.dataflow.sample.timeseriesflow.metrics.BB;
import com.google.dataflow.sample.timeseriesflow.metrics.MA;
import com.google.dataflow.sample.timeseriesflow.metrics.RSI;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Wrapper class used to deploy pipelines with all available metrics. Currently includes:
 *
 * <p>Type 1 {@link TSNumericCombiner}
 *
 * <p>Type 2 {@link RSI},{@link MA},{@link BB}
 */
@Experimental
public class AllMetricsWithDefaults {

  public static List<CombineFn<TSDataPoint, TSAccum, TSAccum>> getAllType1Combiners() {
    return ImmutableList.of(new TSNumericCombiner());
  }

  public static ImmutableList<
          PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>>>
      getAllType2Computations() {
    return ImmutableList.of(
        RSI.toBuilder()
            .setAverageComputationMethod(RSI.AverageComputationMethod.ALL)
            .build()
            .create(),
        MA.toBuilder()
            .setAverageComputationMethod(MA.AverageComputationMethod.SIMPLE_MOVING_AVERAGE)
            .build()
            .create(),
        MA.toBuilder()
            .setAverageComputationMethod(MA.AverageComputationMethod.EXPONENTIAL_MOVING_AVERAGE)
            .setWeight(BigDecimal.valueOf(2D / (3D + 1D)))
            .build()
            .create(),
        BB.toBuilder()
            .setAverageComputationMethod(BB.AverageComputationMethod.EXPONENTIAL_MOVING_AVERAGE)
            .setWeight(BigDecimal.valueOf(2D / (3D + 1D)))
            .setDevFactor(2)
            .build()
            .create(),
        BB.toBuilder()
            .setAverageComputationMethod(BB.AverageComputationMethod.SIMPLE_MOVING_AVERAGE)
            .setDevFactor(2)
            .build()
            .create());
  }
}
