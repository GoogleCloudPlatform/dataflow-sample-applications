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
package com.google.dataflow.sample.timeseriesflow.metrics.complex.rsi;

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation.ComputeType;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Given a span of aggregates compute RSI */
@AutoValue
public abstract class RSI implements Serializable {

  public abstract AverageComputationMethod getAverageComputationMethod();

  public static Builder toBuilder() {
    return new AutoValue_RSI.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAverageComputationMethod(AverageComputationMethod duration);

    public abstract RSI build();
  }

  public RSIComputation create() {
    return new RSIComputation(this);
  }

  /**
   * Compute the value of {100 - ( 100 / 1 + ((Avg Gain) / Abs(Avg Loss))) (Avg % Gain) AG (Avg
   * %Loss) AL} {if AL == 0 then RSI== 100}
   *
   * <p>TODO Add full support for BigDecimal
   */
  @TypeTwoComputation(computeType = ComputeType.SINGLE_KEY)
  public static class RSIComputation
      extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

    RSI rsi;

    public RSIComputation(RSI rsi) {
      this.rsi = rsi;
    }

    public RSIComputation(@Nullable String name, RSI rsi) {
      super(name);
      this.rsi = rsi;
    }

    @Override
    public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {

      // Part one of computation, compute the Sum and Loss
      PCollection<TSAccum> gainSumComputation =
          input.apply(Values.create()).apply(ParDo.of(new ComputeSumUpSumDownWithCount()));

      // Part two compute the Moving Average RSI

      return gainSumComputation.apply(ParDo.of(new GenerateMovingAverageRSI()));
    }
  }

  private static class GenerateMovingAverageRSI extends DoFn<TSAccum, KV<TSKey, TSAccum>> {
    @ProcessElement
    public void process(@Element TSAccum input, OutputReceiver<KV<TSKey, TSAccum>> o) {

      AccumRSIBuilder rsiBuilder = new AccumRSIBuilder(input);

      Long count = (long) rsiBuilder.getMovementCount().getIntVal();
      Double upSum = rsiBuilder.getSumUp().getDoubleVal();
      Double downSum = rsiBuilder.getSumDown().getDoubleVal();

      Double ag = (upSum == 0) ? 0D : upSum / count;
      Double al = (downSum == 0) ? 0D : Math.abs(downSum / count);

      rsiBuilder
          .setABSMovingAverageGain(CommonUtils.createNumData(ag))
          .setABSMovingAverageLoss(CommonUtils.createNumData(al));

      // RSI is 100 if ag is 0
      if (ag == 0 && al == 0) {
        o.output(
            KV.of(
                input.getKey(),
                rsiBuilder
                    .setRelativeStrength(CommonUtils.createNumData(0d))
                    .setRelativeStrengthIndicator(CommonUtils.createNumData(0d))
                    .build()));
        return;
      }

      // RSI is 100 if al is 0
      if (ag == 0) {
        o.output(
            KV.of(
                input.getKey(),
                rsiBuilder
                    .setRelativeStrength(CommonUtils.createNumData(100d))
                    .setRelativeStrengthIndicator(CommonUtils.createNumData(0d))
                    .build()));
        return;
      }

      // RSI is 100 if al is 0
      if (al == 0) {
        o.output(
            KV.of(
                input.getKey(),
                rsiBuilder
                    .setRelativeStrength(CommonUtils.createNumData(0d))
                    .setRelativeStrengthIndicator(CommonUtils.createNumData(100d))
                    .build()));
        return;
      }

      // rs = ag/al or 100 if al is 0
      Double rs = ag / al;

      // RSI = 100 – 100 / ( 1 + RS)
      Double rsi = 100 - (100 / (1 + rs));

      o.output(
          KV.of(
              input.getKey(),
              rsiBuilder
                  .setRelativeStrength(CommonUtils.createNumData(rs))
                  .setRelativeStrengthIndicator(CommonUtils.createNumData(rsi))
                  .build()));
    }
  }

  /** Only one method supported. */
  public enum AverageComputationMethod {
    //    MOVING_AVERAGE,
    //    EXPONENTIAL_MOVING_AVERAGE,
    //    WILDERS_MOVING_AVERAGE,
    ALL
  }
}
