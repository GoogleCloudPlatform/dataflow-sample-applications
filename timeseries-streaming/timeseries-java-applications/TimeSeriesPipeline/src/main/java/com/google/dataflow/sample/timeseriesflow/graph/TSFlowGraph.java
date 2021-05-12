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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

@AutoValue
/** TSFlow Graph and graph component holder */
public abstract class TSFlowGraph {

  // Verified Raw Collection Type
  public abstract PCollection<KV<TSKey, TSDataPoint>> getVerifiedTSDataPoints();

  // Verified Raw data separated by types
  public abstract PCollectionTuple getDatabyType();

  // Type 1 computations
  public abstract PCollection<KV<TSKey, TSAccum>> getType1Computations();

  // Type 2 computations
  public abstract PCollection<KV<TSKey, TSAccum>> getType2Computations();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_TSFlowGraph.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setVerifiedTSDataPoints(
        PCollection<KV<TSKey, TSDataPoint>> newVerifiedTSDataPoints);

    public abstract Builder setDatabyType(PCollectionTuple newDatabyType);

    public abstract Builder setType1Computations(
        PCollection<KV<TSKey, TSAccum>> newType1Computations);

    public abstract Builder setType2Computations(
        PCollection<KV<TSKey, TSAccum>> newType2Computations);

    public abstract TSFlowGraph build();
  }

  // Merged Type 1 & 2 computations

}
