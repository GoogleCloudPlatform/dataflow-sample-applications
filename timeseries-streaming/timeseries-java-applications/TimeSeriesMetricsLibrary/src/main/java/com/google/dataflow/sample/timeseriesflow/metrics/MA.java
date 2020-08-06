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
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class MA implements Serializable {
  public abstract AverageComputationMethod getAverageComputationMethod();

  public static Builder toBuilder() {
    return new AutoValue_MA.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAverageComputationMethod(AverageComputationMethod duration);

    public abstract MA build();
  }

  public MAComputation create() {
    return new MAComputation(this);
  }

  /**
   * Compute Moving Average
   *
   * <p>TODO Add full support for BigDecimal
   */
  public static class MAComputation
      extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

    MA ma;

    public MAComputation(MA ma) {
      this.ma = ma;
    }

    public MAComputation(@Nullable String name, MA ma) {
      super(name);
      this.ma = ma;
    }

    @Override
    public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {

      // Part one of computation, compute the Sum
      PCollection<TSAccum> sumComputation =
          input.apply(Values.create()).apply(ParDo.of(new ComputeSumWithCount()));

      // Part two compute the Moving Average MA
      return sumComputation.apply(ParDo.of(new GenerateMovingAverageMA()));
    }
  }

  private static class GenerateMovingAverageMA extends DoFn<TSAccum, KV<TSKey, TSAccum>> {
    @ProcessElement
    public void process(@Element TSAccum input, OutputReceiver<KV<TSKey, TSAccum>> o) {

      AccumMABuilder maBuilder = new AccumMABuilder(input);

      Long count = (long) maBuilder.getMovementCount().getIntVal();
      Double sum = maBuilder.getSum().getDoubleVal();

      Double avg = (sum == 0) ? 0D : sum / count;
      maBuilder.setABSMovingAverage(CommonUtils.createNumData(avg));

      o.output(KV.of(input.getKey(), maBuilder.build()));
    }
  }

  /** Only one method supported for now. */
  public enum AverageComputationMethod {
    //    MOVING_AVERAGE,
    //    EXPONENTIAL_MOVING_AVERAGE,
    //    WILDERS_MOVING_AVERAGE,
    ALL
  }
}
