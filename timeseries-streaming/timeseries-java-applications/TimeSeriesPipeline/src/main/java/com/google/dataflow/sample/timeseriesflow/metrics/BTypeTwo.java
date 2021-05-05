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

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Basic Type 2 metrics are a simple serializable function which takes a TSAccumSequence and outputs
 * a metric. This class is not intended for direct usage.
 */
@AutoValue
public abstract class BTypeTwo
    extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

  public abstract Builder toBuilder();

  @Nullable
  public abstract Contextful<Fn<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>> getMapping();

  public BTypeTwo via(ProcessFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> fn) {
    return this.toBuilder().setMapping(Contextful.fn(fn)).build();
  }

  public static Builder builder() {
    return new AutoValue_BTypeTwo.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract BTypeTwo build();

    public abstract Builder setMapping(
        Contextful<Fn<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>> value);
  }

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
    return input.apply(
        MapElements.into(CommonUtils.getKvTSAccumTypedescritors()).via(getMapping()));
  }
}
