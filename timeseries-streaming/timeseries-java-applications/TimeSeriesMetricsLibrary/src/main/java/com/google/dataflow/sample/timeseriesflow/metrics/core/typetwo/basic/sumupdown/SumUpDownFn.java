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
package com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.sumupdown;

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
import java.util.Iterator;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

@Experimental
public class SumUpDownFn extends BTypeTwoFn {

  /** Options for {@link SumUpDownFn} fn. */
  public interface SumUpDownOptions extends PipelineOptions {}

  public SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> getFunction(
      PipelineOptions options) {

    return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
        element -> {
          Iterator<TSAccum> it = element.getValue().getAccumsList().iterator();

          AccumCoreNumericBuilder current = new AccumCoreNumericBuilder(it.next());

          BigDecimal up = new BigDecimal(0);
          BigDecimal down = new BigDecimal(0);

          while (it.hasNext()) {

            AccumCoreNumericBuilder next = new AccumCoreNumericBuilder(it.next());
            BigDecimal currentLastValue =
                TSDataUtils.getBigDecimalFromData(current.getLastOrNull());
            BigDecimal nextLastValue = TSDataUtils.getBigDecimalFromData(next.getLastOrNull());

            BigDecimal diff = nextLastValue.subtract(currentLastValue);

            int compare = diff.compareTo(new BigDecimal(0));

            if (compare > 0) {
              up = up.add(diff);
            } else {
              down = down.add(diff);
            }
            current = next;
          }

          AccumSumUpDownBuilder builder =
              new AccumSumUpDownBuilder(TSAccum.newBuilder().setKey(element.getKey()).build());

          builder
              .setSumUp(CommonUtils.createNumData(up.doubleValue()))
              .setSumDown(CommonUtils.createNumData(down.doubleValue()))
              .setMovementCount(CommonUtils.createNumData(element.getValue().getAccumsCount()));

          return KV.of(element.getKey(), builder.build());
        };
  }
  /** Builder for the {@link SumUpDownFn} type 2 computation data store */
  @Experimental
  public static class AccumSumUpDownBuilder extends AccumCoreMetadataBuilder {
    public AccumSumUpDownBuilder(TSAccum tsAccum) {
      super(tsAccum);
    }

    public Data getSumUp() {
      return getValueOrNull(FsiTechnicalIndicators.SUM_UP_MOVEMENT.name());
    }

    public Data getSumDown() {
      return getValueOrNull(FsiTechnicalIndicators.SUM_DOWN_MOVEMENT.name());
    }

    public Data getMovementCount() {
      return getValueOrNull(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name());
    }

    AccumSumUpDownBuilder setSumUp(Data data) {
      setValue(FsiTechnicalIndicators.SUM_UP_MOVEMENT.name(), data);
      return this;
    }

    AccumSumUpDownBuilder setSumDown(Data data) {
      setValue(FsiTechnicalIndicators.SUM_DOWN_MOVEMENT.name(), data);
      return this;
    }

    AccumSumUpDownBuilder setMovementCount(Data data) {
      setValue(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name(), data);
      return this;
    }
  }
}
