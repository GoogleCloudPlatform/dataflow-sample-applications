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
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class StdDev implements Serializable {

  public static Builder toBuilder() {
    return new AutoValue_StdDev.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract StdDev build();
  }

  public StdDevComputation create() {
    return new StdDevComputation(this);
  }

  /**
   * Compute StdDev
   *
   */
  public static class StdDevComputation
      extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

    StdDev stdDev;

    public StdDevComputation(StdDev ma) {
      this.stdDev = stdDev;
    }

    public StdDevComputation(@Nullable String name, StdDev stdDev) {
      super(name);
      this.stdDev = stdDev;
    }

    @Override
    public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
      PCollection<KV<TSKey, TSAccum>> result =
          input.apply(Values.create()).apply(ParDo.of(new ComputeStdDevDoFn()));
      return result;
    }
  }
}
