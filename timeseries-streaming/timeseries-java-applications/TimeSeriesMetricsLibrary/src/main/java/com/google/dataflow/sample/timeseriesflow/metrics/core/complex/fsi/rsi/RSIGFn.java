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
package com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.rsi;

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwo;
import com.google.dataflow.sample.timeseriesflow.metrics.CTypeTwo;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.sumupdown.SumUpDownFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.sumupdown.SumUpDownFn.AccumSumUpDownBuilder;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@Experimental
public class RSIGFn extends CTypeTwo {

  public interface RSIOptions extends TSFlowOptions {};

  PCollection<KV<TSKey, TSAccum>> results;

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
    // RSI requires Sum UP and Sum Down Type 2 computation.
    PCollection<KV<TSKey, TSAccum>> sumUpDown =
        input.apply(
            BTypeTwo.builder()
                .setMapping(new SumUpDownFn().getContextualFn(input.getPipeline().getOptions()))
                .build());

    return sumUpDown.apply(ParDo.of(new GenerateMovingAverageRSI()));
  }

  public List<Class<? extends BTypeOne>> requiredTypeOne() {
    return null;
  }

  @Override
  public List<String> excludeFromOutput() {
    return null;
  }

  private static class GenerateMovingAverageRSI
      extends DoFn<KV<TSKey, TSAccum>, KV<TSKey, TSAccum>> {
    @ProcessElement
    public void process(@Element KV<TSKey, TSAccum> input, OutputReceiver<KV<TSKey, TSAccum>> o) {

      AccumRSIBuilder rsiBuilder = new AccumRSIBuilder(input.getValue());

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

  public static class AccumRSIBuilder extends AccumSumUpDownBuilder {
    public AccumRSIBuilder(TSAccum tsAccum) {
      super(tsAccum);
    }

    public Data getABSMovingAverageGain() {
      return getValueOrNull(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_GAIN.name());
    }

    public Data getABSMovingAverageLoss() {
      return getValueOrNull(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_LOSS.name());
    }

    public Data getRelativeStrength() {
      return getValueOrNull(FsiTechnicalIndicators.RELATIVE_STRENGTH.name());
    }

    public Data getRelativeStrengthIndicator() {
      return getValueOrNull(FsiTechnicalIndicators.RELATIVE_STRENGTH_INDICATOR.name());
    }

    public AccumRSIBuilder setABSMovingAverageGain(Data data) {
      setValue(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_GAIN.name(), data);
      return this;
    }

    public AccumRSIBuilder setABSMovingAverageLoss(Data data) {
      setValue(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_LOSS.name(), data);
      return this;
    }

    public AccumRSIBuilder setRelativeStrength(Data data) {
      setValue(FsiTechnicalIndicators.RELATIVE_STRENGTH.name(), data);
      return this;
    }

    public AccumRSIBuilder setRelativeStrengthIndicator(Data data) {
      setValue(FsiTechnicalIndicators.RELATIVE_STRENGTH_INDICATOR.name(), data);
      return this;
    }
  }
}
