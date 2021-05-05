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
package com.google.dataflow.sample.timeseriesflow.metrics;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Complex Type 2 metrics require access to the TSFlow graph to create multi stage computations. If
 * a computation is a simple f(KV<TSKey, TSAccumSequence>) -> KV<TSKey, TSAccum> make use of {@link
 * BTypeTwo}
 */
public abstract class CTypeTwo
    extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

  /**
   * Some type 2 computations may need specialized type 1 computations. {@link
   * com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations} class will attach any
   * provided by this list. If none required return an empty list.
   */
  public abstract List<Class<? extends BTypeOne>> requiredTypeOne();

  public abstract List<String> excludeFromOutput();
}
