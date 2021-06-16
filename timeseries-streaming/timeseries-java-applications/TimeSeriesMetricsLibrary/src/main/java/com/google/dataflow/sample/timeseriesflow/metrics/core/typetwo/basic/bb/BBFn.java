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
package com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.bb;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwoFn;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.StatisticalFormulas;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Generate middle upper and bottom bands for Bollinger Bars
 *
 * <p>TODO Add notes on defaults. TODO Add notes for EMA.
 */
@Experimental
public class BBFn extends BTypeTwoFn {

  /** Options for {@link BBFn} fn. */
  public interface BBOptions extends PipelineOptions {

    Double getBBAlpha();

    void setBBAlpha(Double alpha);

    BBAvgComputeMethod getBBAvgComputeMethod();

    void setBBAvgComputeMethod(BBAvgComputeMethod avgComputeMethod);

    Integer getBBDevFactor();

    void setBBDevFactor(Integer integer);
  }

  public SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> getFunction(
      PipelineOptions options) {

    BBOptions bbOptions = options.as(BBOptions.class);
    // Enum has issue with serlize in DoFnWithExecutionInformation keep enum outside Fn

    // Optional values, or values with defaults
    int devFactor = Optional.ofNullable(bbOptions.getBBDevFactor()).orElse(2);
    BBAvgComputeMethod bbAvgComputeMethod =
        Optional.ofNullable(bbOptions.getBBAvgComputeMethod())
            .orElse(BBAvgComputeMethod.SIMPLE_MOVING_AVERAGE);

    // Required values without defaults
    if (bbOptions.getBBAvgComputeMethod().equals(BBAvgComputeMethod.EXPONENTIAL_MOVING_AVERAGE)) {
      Preconditions.checkNotNull(bbOptions.getBBAlpha());
    }
    Double alpha = bbOptions.getBBAlpha();

    if (bbAvgComputeMethod == BBAvgComputeMethod.EXPONENTIAL_MOVING_AVERAGE) {

      return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
          element -> {
            Iterator<TSAccum> itAvg = element.getValue().getAccumsList().iterator();

            Iterator<TSAccum> itStdDev = element.getValue().getAccumsList().iterator();

            AccumBBBuilder bbBuilder =
                new AccumBBBuilder(TSAccum.newBuilder().setKey(element.getKey()).build());

            BigDecimal movingAverage;

            movingAverage =
                StatisticalFormulas.computeExponentialMovingAverage(
                    itAvg, BigDecimal.valueOf(alpha));
            BigDecimal stdDev = StatisticalFormulas.computeStandardDeviation(itStdDev);

            bbBuilder
                .setMovementCount(CommonUtils.createNumData(element.getValue().getAccumsCount()))
                .setMidBandEMA(CommonUtils.createNumData(movingAverage.doubleValue()))
                .setUpperBandEMA(
                    CommonUtils.createNumData(
                        movingAverage
                            .add(stdDev.multiply(BigDecimal.valueOf(devFactor)))
                            .doubleValue()))
                .setBottomBandEMA(
                    CommonUtils.createNumData(
                        movingAverage
                            .subtract(stdDev.multiply(BigDecimal.valueOf(devFactor)))
                            .doubleValue()));
            return KV.of(element.getKey(), bbBuilder.build());
          };
    } else // By default we use Simple Moving Average
    {
      return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
          element -> {
            Iterator<TSAccum> itAvg = element.getValue().getAccumsList().iterator();

            Iterator<TSAccum> itStdDev = element.getValue().getAccumsList().iterator();

            AccumBBBuilder bbBuilder =
                new AccumBBBuilder(TSAccum.newBuilder().setKey(element.getKey()).build());

            BigDecimal movingAverage;

            movingAverage = StatisticalFormulas.computeSimpleMovingAverage(itAvg);
            BigDecimal stdDev = StatisticalFormulas.computeStandardDeviation(itStdDev);

            bbBuilder
                .setMovementCount(CommonUtils.createNumData(element.getValue().getAccumsCount()))
                .setMidBandSMA(CommonUtils.createNumData(movingAverage.doubleValue()))
                .setUpperBandSMA(
                    CommonUtils.createNumData(
                        movingAverage
                            .add(stdDev.multiply(BigDecimal.valueOf(devFactor)))
                            .doubleValue()))
                .setBottomBandSMA(
                    CommonUtils.createNumData(
                        movingAverage
                            .subtract(stdDev.multiply(BigDecimal.valueOf(devFactor)))
                            .doubleValue()));
            return KV.of(element.getKey(), bbBuilder.build());
          };
    }
  }

  public enum BBAvgComputeMethod {
    SIMPLE_MOVING_AVERAGE,
    EXPONENTIAL_MOVING_AVERAGE
  }

  /** Builder for the {@link BBFn} type 2 computation data store */
  @Experimental
  public static class AccumBBBuilder extends AccumCoreMetadataBuilder {
    public AccumBBBuilder(TSAccum tsAccum) {
      super(tsAccum);
    }

    public Data getMovementCount() {
      return getValueOrNull(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name());
    }

    public Data getMidBandSMA() {
      return getValueOrNull(FsiTechnicalIndicators.BB_MIDDLE_BAND_SMA.name());
    }

    public Data getUpperBandSMA() {
      return getValueOrNull(FsiTechnicalIndicators.BB_UPPER_BAND_SMA.name());
    }

    public Data getBottomBandSMA() {
      return getValueOrNull(FsiTechnicalIndicators.BB_BOTTOM_BAND_SMA.name());
    }

    public Data getMidBandEMA() {
      return getValueOrNull(FsiTechnicalIndicators.BB_MIDDLE_BAND_EMA.name());
    }

    public Data getUpperBandEMA() {
      return getValueOrNull(FsiTechnicalIndicators.BB_UPPER_BAND_EMA.name());
    }

    public Data getBottomBandEMA() {
      return getValueOrNull(FsiTechnicalIndicators.BB_BOTTOM_BAND_EMA.name());
    }

    public AccumBBBuilder setMovementCount(Data data) {
      setValue(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name(), data);
      return this;
    }

    public AccumBBBuilder setMidBandSMA(Data data) {
      setValue(FsiTechnicalIndicators.BB_MIDDLE_BAND_SMA.name(), data);
      return this;
    }

    public AccumBBBuilder setUpperBandSMA(Data data) {
      setValue(FsiTechnicalIndicators.BB_UPPER_BAND_SMA.name(), data);
      return this;
    }

    public AccumBBBuilder setBottomBandSMA(Data data) {
      setValue(FsiTechnicalIndicators.BB_BOTTOM_BAND_SMA.name(), data);
      return this;
    }

    public AccumBBBuilder setMidBandEMA(Data data) {
      setValue(FsiTechnicalIndicators.BB_MIDDLE_BAND_EMA.name(), data);
      return this;
    }

    public AccumBBBuilder setUpperBandEMA(Data data) {
      setValue(FsiTechnicalIndicators.BB_UPPER_BAND_EMA.name(), data);
      return this;
    }

    public AccumBBBuilder setBottomBandEMA(Data data) {
      setValue(FsiTechnicalIndicators.BB_BOTTOM_BAND_EMA.name(), data);
      return this;
    }
  }
}
