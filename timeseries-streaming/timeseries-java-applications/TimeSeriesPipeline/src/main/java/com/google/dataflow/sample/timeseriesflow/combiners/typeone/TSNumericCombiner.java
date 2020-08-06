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
package com.google.dataflow.sample.timeseriesflow.combiners.typeone;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data.DataPointCase;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.combiners.TSCombiner;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
/** Base type 1 combiner, which computes First / Last / Min / Max / Count and Sum. */
public class TSNumericCombiner extends TSBaseCombiner implements TSCombiner {

  public static TSNumericCombiner combine() {
    return new TSNumericCombiner();
  }

  @Override
  public TSAccum mergeTypedDataAccum(TSAccum a, TSAccum b) {

    AccumCoreNumericBuilder aBuilder = new AccumCoreNumericBuilder(a);
    AccumCoreNumericBuilder bBuilder = new AccumCoreNumericBuilder(b);

    aBuilder.setSum(
        CommonUtils.sumNumericDataNullAsZero(aBuilder.getSumOrNull(), bBuilder.getSumOrNull()));
    aBuilder.setMin(TSDataUtils.findMinData(aBuilder.getMinOrNull(), bBuilder.getMinOrNull()));
    aBuilder.setMax(TSDataUtils.findMaxValue(aBuilder.getMaxOrNull(), bBuilder.getMaxOrNull()));

    return aBuilder.build();
  }

  @Override
  public TSAccum addTypeSpecificInput(TSAccum accumulator, TSDataPoint dataPoint) {
    AccumCoreNumericBuilder coreNumeric = new AccumCoreNumericBuilder(accumulator);

    Preconditions.checkArgument(
        !dataPoint.getData().getDataPointCase().equals(DataPointCase.DATAPOINT_NOT_SET));

    coreNumeric.setSum(
        CommonUtils.sumNumericDataNullAsZero(coreNumeric.getSumOrNull(), dataPoint.getData()));

    coreNumeric.setMin(TSDataUtils.findMinData(coreNumeric.getMinOrNull(), dataPoint.getData()));

    coreNumeric.setMax(TSDataUtils.findMaxValue(coreNumeric.getMaxOrNull(), dataPoint.getData()));

    return coreNumeric.build();
  }
}
