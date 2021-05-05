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
package com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.stddev;

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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

@Experimental
public class StdDevFn extends BTypeTwoFn {

  /** Options for {@link StdDevFn} fn. */
  public static interface StdDevOptions extends PipelineOptions {}

  public SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> getFunction(
      PipelineOptions options) {

    return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
        (element) -> {
          Iterator<TSAccum> it = element.getValue().getAccumsList().iterator();

          BigDecimal stdDev = StatisticalFormulas.computeStandardDeviation(it);

          AccumStdDevBuilder stdDevBuilder =
              new AccumStdDevBuilder(TSAccum.newBuilder().setKey(element.getKey()).build());

          stdDevBuilder
              .setMovementCount(CommonUtils.createNumData(element.getValue().getAccumsCount()))
              .setStdDev(CommonUtils.createNumData(stdDev.doubleValue()));

          return KV.of(element.getKey(), stdDevBuilder.build());
        };
  }

  /** Builder for the {@link MA} type 2 computation data store */
  @Experimental
  public static class AccumStdDevBuilder extends AccumCoreMetadataBuilder {
    public AccumStdDevBuilder(TSAccum tsAccum) {
      super(tsAccum);
    }

    public Data getMovementCount() {
      return getValueOrNull(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name());
    }

    public Data getStdDev() {
      return getValueOrNull(FsiTechnicalIndicators.STANDARD_DEVIATION.name());
    }

    public AccumStdDevBuilder setMovementCount(Data data) {
      setValue(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name(), data);
      return this;
    }

    public AccumStdDevBuilder setStdDev(Data data) {
      setValue(FsiTechnicalIndicators.STANDARD_DEVIATION.name(), data);
      return this;
    }
  }
}
