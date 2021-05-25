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
package com.google.dataflow.sample.timeseriesflow.metrics.basic.bb;

import static com.google.dataflow.sample.timeseriesflow.metrics.basic.bb.BB.AverageComputationMethod.EXPONENTIAL_MOVING_AVERAGE;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.StatisticalFormulas;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** Generate middle upper and bottom bands for Bollinger Bars {@link TSAccumSequence} */
class ComputeBollingerBandsDoFn extends DoFn<TSAccumSequence, KV<TSKey, TSAccum>> {
  private final BigDecimal alpha;
  private final BB.AverageComputationMethod averageComputationMethod;
  private final Integer devFactor;

  public ComputeBollingerBandsDoFn(
      BB.AverageComputationMethod averageComputationMethod, BigDecimal alpha, Integer devFactor) {
    this.alpha = alpha;
    this.averageComputationMethod = averageComputationMethod;
    this.devFactor = devFactor;
  }

  @ProcessElement
  public void process(ProcessContext pc, OutputReceiver<KV<TSKey, TSAccum>> o) {

    Iterator<TSAccum> itAvg = Objects.requireNonNull(pc.element()).getAccumsList().iterator();

    Iterator<TSAccum> itStdDev = Objects.requireNonNull(pc.element()).getAccumsList().iterator();

    AccumBBBuilder bbBuilder =
        new AccumBBBuilder(
            TSAccum.newBuilder().setKey(Objects.requireNonNull(pc.element()).getKey()).build());

    BigDecimal movingAverage;

    if (averageComputationMethod == EXPONENTIAL_MOVING_AVERAGE) {
      movingAverage = StatisticalFormulas.computeExponentialMovingAverage(itAvg, this.alpha);
      BigDecimal stdDev = StatisticalFormulas.computeStandardDeviation(itStdDev);
      BigDecimal stdDevDelta = stdDev.multiply(BigDecimal.valueOf(this.devFactor));

      bbBuilder
          .setMovementCount(
              CommonUtils.createNumData(Objects.requireNonNull(pc.element()).getAccumsCount()))
          .setMidBandEMA(CommonUtils.createNumData(movingAverage.doubleValue()))
          .setUpperBandEMA(CommonUtils.createNumData(movingAverage.add(stdDevDelta).doubleValue()))
          .setBottomBandEMA(
              CommonUtils.createNumData(movingAverage.subtract(stdDevDelta).doubleValue()));
    } else { // By default we use Simple Moving Average
      movingAverage = StatisticalFormulas.computeSimpleMovingAverage(itAvg);
      BigDecimal stdDev = StatisticalFormulas.computeStandardDeviation(itStdDev);
      BigDecimal stdDevDelta = stdDev.multiply(BigDecimal.valueOf(this.devFactor));

      bbBuilder
          .setMovementCount(
              CommonUtils.createNumData(Objects.requireNonNull(pc.element()).getAccumsCount()))
          .setMidBandSMA(CommonUtils.createNumData(movingAverage.doubleValue()))
          .setUpperBandSMA(CommonUtils.createNumData(movingAverage.add(stdDevDelta).doubleValue()))
          .setBottomBandSMA(
              CommonUtils.createNumData(movingAverage.subtract(stdDevDelta).doubleValue()));
    }

    o.output(KV.of(Objects.requireNonNull(pc.element()).getKey(), bbBuilder.build()));
  }
}
