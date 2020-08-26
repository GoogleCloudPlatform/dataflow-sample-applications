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

import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSNumericCombiner;
import com.google.dataflow.sample.timeseriesflow.metrics.MA;
import com.google.dataflow.sample.timeseriesflow.metrics.MA.AverageComputationMethod;
import com.google.dataflow.sample.timeseriesflow.metrics.RSI;
import com.google.dataflow.sample.timeseriesflow.transforms.GenerateComputations;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Wrapper class used to deploy pipelines with all available metrics. Currently includes:
 *
 * <p>Type 1 {@link TSNumericCombiner}
 *
 * <p>Type 2 {@link RSI},{@link MA}
 */
@Experimental
public class AllMetricsGeneratorWithDefaults {

  /** @return {@link GenerateComputations.Builder} */
  public static GenerateComputations.Builder getGenerateComputationsWithAllKnownMetrics() {

    return GenerateComputations.builder()
        .setType1NumericComputations(ImmutableList.of(new TSNumericCombiner()))
        .setType2NumericComputations(
            ImmutableList.of(
                RSI.toBuilder()
                    .setAverageComputationMethod(RSI.AverageComputationMethod.ALL)
                    .build()
                    .create(),
                MA.toBuilder()
                    .setAverageComputationMethod(AverageComputationMethod.SIMPLE_MOVING_AVERAGE)
                    .build()
                    .create(),
                MA.toBuilder()
                        .setAverageComputationMethod(AverageComputationMethod.EXPONENTIAL_MOVING_AVERAGE)
                        .build()
                        .create()));
  }
}
