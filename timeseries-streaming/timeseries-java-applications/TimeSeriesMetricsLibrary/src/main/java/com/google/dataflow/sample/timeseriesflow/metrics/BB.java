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
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class BB implements Serializable {
  public abstract AverageComputationMethod getAverageComputationMethod();

  public abstract BigDecimal getWeight();

  public abstract Integer getDevFactor();

  public static Builder toBuilder() {
    return new AutoValue_BB.Builder().setWeight(BigDecimal.ZERO);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAverageComputationMethod(AverageComputationMethod method);

    public abstract Builder setWeight(BigDecimal alpha);

    public abstract Builder setDevFactor(Integer devFactor);

    public abstract BB build();
  }

  public BBComputation create() {
    return new BBComputation(this);
  }

  /**
   * Compute Bollinger Bands
   *
   * <p>TODO Add full support for BigDecimal
   */
  public static class BBComputation
      extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

    BB bb;

    public BBComputation(BB bb) {
      this.bb = bb;
    }

    public BBComputation(@Nullable String name, BB bb) {
      super(name);
      this.bb = bb;
    }

    @Override
    public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
      PCollection<KV<TSKey, TSAccum>> result =
          input
              .apply(Values.create())
              .apply(
                  ParDo.of(
                      new ComputeBollingerBandsDoFn(
                          this.bb.getAverageComputationMethod(),
                          this.bb.getWeight(),
                          this.bb.getDevFactor())));
      return result;
    }
  }

  public enum AverageComputationMethod {
    SIMPLE_MOVING_AVERAGE,
    EXPONENTIAL_MOVING_AVERAGE
  }
}
