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
package com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.logrtn;

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwoFn;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

@Experimental
public class LogRtnFn extends BTypeTwoFn {

  /** Options for {@link LogRtnFn} fn. */
  public interface LogRtnOptions extends PipelineOptions {}

  public SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> getFunction(
      PipelineOptions options) {

    return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
        element -> {
          Iterator<TSAccum> it = element.getValue().getAccumsList().iterator();

          TSAccum last = it.next();
          TSAccum penultimate = null;

          while (it.hasNext()) {
            penultimate = last;
            last = it.next();
          }

          AccumLogRtnBuilder logRtnBuilder =
              new AccumLogRtnBuilder(TSAccum.newBuilder().setKey(element.getKey()).build());

          if (penultimate == null) {
            return KV.of(
                element.getKey(), logRtnBuilder.setLogRtn(CommonUtils.createNumData(0D)).build());
          }

          AccumCoreNumericBuilder lead = new AccumCoreNumericBuilder(last);
          AccumCoreNumericBuilder lag = new AccumCoreNumericBuilder(penultimate);

          BigDecimal bdLead = TSDataUtils.getBigDecimalFromData(lead.getLastOrNull());
          BigDecimal bdlag = TSDataUtils.getBigDecimalFromData(lag.getLastOrNull());

          if (!bdLead.equals(BigDecimal.ZERO) && !bdlag.equals(BigDecimal.ZERO)) {

            BigDecimal diff = bdLead.divide(bdlag, 9, RoundingMode.CEILING);

            // TODO use BigDecimal log
            Double result = Math.log(Math.abs(diff.doubleValue()));

            logRtnBuilder.setLogRtn(
                CommonUtils.createNumData(
                    (Double.isInfinite(result) || result.isNaN()) ? 0D : result));
          } else {

            logRtnBuilder.setLogRtn(CommonUtils.createNumData(0D));
          }
          return KV.of(element.getKey(), logRtnBuilder.build());
        };
  }

  /** Builder for the {@link LogRtn} type 2 computation data store */
  @Experimental
  public static class AccumLogRtnBuilder extends AccumCoreMetadataBuilder {
    public AccumLogRtnBuilder(TSAccum tsAccum) {
      super(tsAccum);
    }

    public Data getLogRtn() {
      return getValueOrNull(FsiTechnicalIndicators.LOG_RTN.name());
    }

    public AccumLogRtnBuilder setLogRtn(Data data) {
      setValue(FsiTechnicalIndicators.LOG_RTN.name(), data);
      return this;
    }
  }
}
