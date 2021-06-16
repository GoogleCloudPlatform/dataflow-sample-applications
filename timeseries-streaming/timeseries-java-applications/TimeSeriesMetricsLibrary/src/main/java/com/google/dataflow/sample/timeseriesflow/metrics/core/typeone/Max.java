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
package com.google.dataflow.sample.timeseriesflow.metrics.core.typeone;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import org.apache.beam.sdk.options.PipelineOptions;

public class Max extends BTypeOne {

  public interface MaxOptions extends PipelineOptions {};

  @Override
  public TSAccum addInput(TSAccum accumulator, TSDataPoint dataPoint) {
    AccumCoreNumericBuilder coreNumeric = new AccumCoreNumericBuilder(accumulator);
    coreNumeric.setMax(TSDataUtils.findMaxValue(coreNumeric.getMaxOrNull(), dataPoint.getData()));
    return coreNumeric.build();
  }

  @Override
  public TSAccum mergeDataAccums(TSAccum a, TSAccum b) {
    AccumCoreNumericBuilder aBuilder = new AccumCoreNumericBuilder(a);
    AccumCoreNumericBuilder bBuilder = new AccumCoreNumericBuilder(b);
    aBuilder.setMax(TSDataUtils.findMaxValue(aBuilder.getMaxOrNull(), bBuilder.getMaxOrNull()));

    return aBuilder.build();
  }
}
