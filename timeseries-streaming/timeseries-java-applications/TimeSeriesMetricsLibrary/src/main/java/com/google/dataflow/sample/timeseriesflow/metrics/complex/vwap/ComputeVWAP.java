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
package com.google.dataflow.sample.timeseriesflow.metrics.complex.vwap;

import com.google.api.client.util.Lists;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation.ComputeType;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TypeTwoComputation(computeType = ComputeType.COMPOSITE_KEY)
public class ComputeVWAP
    extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ComputeVWAP.class);

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
    return input.apply("VWAP", ParDo.of(new VWAP()));
  }

  /** */
  public static class VWAP extends DoFn<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> {

    @ProcessElement
    public void process(
        @Element KV<TSKey, TSAccumSequence> accumSequence, OutputReceiver<KV<TSKey, TSAccum>> o) {

      TSKey outputKey =
          accumSequence.getKey().toBuilder().setMinorKeyString(AccumVWAPBuilder.VWAP).build();

      // TSAccumeSequence is in effect a sequence of ohlc's, need to cycle through for the values
      // that span the time boundary

      List<TSAccum> accums =
          Lists.newArrayList(accumSequence.getValue().getAccumsList().iterator());

      o.output(KV.of(accumSequence.getKey(), computeTradeVWAP(accums, outputKey).build()));
    }

    /** All Gap filled values are expected to have been removed before this is computed */
    private static AccumVWAPBuilder computeTradeVWAP(List<TSAccum> accums, TSKey key) {

      BigDecimal cumPriceQty = new BigDecimal(0);
      BigDecimal cumQty = new BigDecimal(0);

      for (TSAccum accum : accums) {

        AccumVWAPBuilder accumDataMap = new AccumVWAPBuilder(accum);

        // VWAP will be set to 0 if all values in the Accum are gap filled.
        // The HB value is set to 1 if all the values are gap filled values
        if (accumDataMap.getPriceIsHB().getIntVal() == 0) {

          Data tradePriceVolume = accumDataMap.getPriceVolume();
          Data qty = accumDataMap.getQuantity();

          if (tradePriceVolume != null) {
            cumPriceQty = cumPriceQty.add(TSDataUtils.getBigDecimalFromData(tradePriceVolume));
          }
          if (qty != null) {
            cumQty = cumQty.add(TSDataUtils.getBigDecimalFromData(qty));
          }
        }
      }

      AccumVWAPBuilder output = new AccumVWAPBuilder(TSAccum.newBuilder().setKey(key).build());

      if (cumPriceQty.doubleValue() == 0 || cumQty.doubleValue() == 0) {
        output.setVWAP(Data.newBuilder().setDoubleVal(0D).build());
      } else {
        output.setVWAP(
            Data.newBuilder().setDoubleVal(cumPriceQty.divide(cumQty, 4).doubleValue()).build());
      }

      return output;
    }
  }
}
