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
package com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma;

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwoFn;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.StatisticalFormulas;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

@Experimental
public class MAFn extends BTypeTwoFn {

  public SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> getFunction(
      PipelineOptions options) {

    MAOptions maOptions = options.as(MAOptions.class);
    if (maOptions.getMAAvgComputeMethod() == MAAvgComputeMethod.EXPONENTIAL_MOVING_AVERAGE) {

      final Double alpha = Optional.ofNullable(maOptions.getMAAvgComputeAlpha()).orElse(0D);

      return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
          (element) -> {
            Iterator<TimeSeriesData.TSAccum> it = element.getValue().getAccumsList().iterator();
            AccumCoreNumericBuilder current;

            BigDecimal ema =
                StatisticalFormulas.computeExponentialMovingAverage(it, BigDecimal.valueOf(alpha));
            AccumMABuilder maBuilder =
                new AccumMABuilder(
                    TimeSeriesData.TSAccum.newBuilder().setKey(element.getKey()).build());
            maBuilder
                .setExponentialMovingAverage(CommonUtils.createNumData(ema.doubleValue()))
                .setMovementCount(CommonUtils.createNumData(element.getValue().getAccumsCount()));

            return KV.of(element.getKey(), maBuilder.build());
          };
    } else {

      return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
          (element) -> {
            Iterator<TSAccum> it = element.getValue().getAccumsList().iterator();

            BigDecimal avg = StatisticalFormulas.computeSimpleMovingAverage(it);

            AccumMABuilder maBuilder =
                new AccumMABuilder(TSAccum.newBuilder().setKey(element.getKey()).build());

            maBuilder
                .setMovementCount(CommonUtils.createNumData(element.getValue().getAccumsCount()))
                .setSimpleMovingAverage(CommonUtils.createNumData(avg.doubleValue()));

            return KV.of(element.getKey(), maBuilder.build());
          };
    }
  }

  public interface MAOptions extends PipelineOptions {

    MAAvgComputeMethod getMAAvgComputeMethod();

    void setMAAvgComputeMethod(MAAvgComputeMethod method);

    Double getMAAvgComputeAlpha();

    void setMAAvgComputeAlpha(Double method);
  }

  public enum MAAvgComputeMethod {
    SIMPLE_MOVING_AVERAGE,
    EXPONENTIAL_MOVING_AVERAGE
  }

  /** Builder for the {@link MAFn} type 2 computation data store */
  @Experimental
  public static class AccumMABuilder extends AccumCoreMetadataBuilder {
    public AccumMABuilder(TSAccum tsAccum) {
      super(tsAccum);
    }

    public Data getSum() {
      return getValueOrNull(FsiTechnicalIndicators.SUM_MOVEMENT.name());
    }

    public Data getMovementCount() {
      return getValueOrNull(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name());
    }

    public Data getSimpleMovingAverage() {
      return getValueOrNull(FsiTechnicalIndicators.SIMPLE_MOVING_AVERAGE.name());
    }

    public Data getWeightedMovingAverage() {
      return getValueOrNull(FsiTechnicalIndicators.WEIGHTED_MOVING_AVERAGE.name());
    }

    public Data getExponentialMovingAverage() {
      return getValueOrNull(FsiTechnicalIndicators.EXPONENTIAL_MOVING_AVERAGE.name());
    }

    public AccumMABuilder setSum(Data data) {
      setValue(FsiTechnicalIndicators.SUM_MOVEMENT.name(), data);
      return this;
    }

    public AccumMABuilder setMovementCount(Data data) {
      setValue(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name(), data);
      return this;
    }

    public AccumMABuilder setSimpleMovingAverage(Data data) {
      setValue(FsiTechnicalIndicators.SIMPLE_MOVING_AVERAGE.name(), data);
      return this;
    }

    public AccumMABuilder setWeightedMovingAverage(Data data) {
      setValue(FsiTechnicalIndicators.WEIGHTED_MOVING_AVERAGE.name(), data);
      return this;
    }

    public AccumMABuilder setExponentialMovingAverage(Data data) {
      setValue(FsiTechnicalIndicators.EXPONENTIAL_MOVING_AVERAGE.name(), data);
      return this;
    }
  }
}
