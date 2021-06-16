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
package com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data.DataPointCase;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn.AccumVWAPBuilder;
import java.math.BigDecimal;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
public class VWAPTypeOneComp extends BTypeOne {
  @Override
  public TSAccum addInput(TSAccum accumulator, TSDataPoint dataPoint) {
    // TODO add common utils for multiply / divide
    AccumVWAPBuilder vwapBuilder = new AccumVWAPBuilder(accumulator);

    Preconditions.checkArgument(
        !dataPoint.getData().getDataPointCase().equals(DataPointCase.DATAPOINT_NOT_SET));

    // If no volume is included then the value of PriceVol is zero

    Data vol =
        dataPoint.getExtendedDataOrDefault(
            AccumVWAPBuilder.VOL, Data.newBuilder().setDoubleVal(0D).build());

    BigDecimal powerVol = TSDataUtils.multiply(dataPoint.getData(), vol);

    BigDecimal sumPriceVol =
        TSDataUtils.getBigDecimalFromData(
                Optional.ofNullable(vwapBuilder.getPriceVolOrNull())
                    .orElse(CommonUtils.createZeroOfType(dataPoint.getData().getDataPointCase())))
            .add(powerVol);

    BigDecimal sumVol =
        TSDataUtils.add(
            Optional.ofNullable(vwapBuilder.getSumVolOrNull())
                .orElse(CommonUtils.createZeroOfType(dataPoint.getData().getDataPointCase())),
            vol);

    vwapBuilder.setPriceVol(CommonUtils.createNumAsStringData(sumPriceVol.toString()));
    vwapBuilder.setSumVol(CommonUtils.createNumAsStringData(sumVol.toString()));

    return vwapBuilder.build();
  }

  @Override
  public TSAccum mergeDataAccums(TSAccum a, TSAccum b) {

    AccumVWAPBuilder aBuilder = new AccumVWAPBuilder(a);
    AccumVWAPBuilder bBuilder = new AccumVWAPBuilder(b);

    aBuilder.setPriceVol(
        CommonUtils.sumNumericDataNullAsZero(
            aBuilder.getPriceVolOrNull(), bBuilder.getPriceVolOrNull()));

    aBuilder.setSumVol(
        CommonUtils.sumNumericDataNullAsZero(
            aBuilder.getSumVolOrNull(), bBuilder.getSumVolOrNull()));

    return aBuilder.build();
  }
}
