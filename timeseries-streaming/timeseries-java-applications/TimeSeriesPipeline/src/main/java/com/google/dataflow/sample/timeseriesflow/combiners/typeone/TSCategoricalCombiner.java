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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.combiners.TSCombiner;
import org.apache.beam.sdk.annotations.Experimental;

/** Not implemented. */
@Experimental
public class TSCategoricalCombiner extends TSBaseCombiner implements TSCombiner {

  // TODO implement in next cycle.
  private TSCategoricalCombiner() {}

  public static TSCategoricalCombiner combine() {
    return new TSCategoricalCombiner();
  }

  @Override
  public TSAccum mergeTypedDataAccum(TSAccum a, TSAccum b) {

    return null;
  }

  @Override
  public TSAccum addTypeSpecificInput(TSAccum accumulator, TSDataPoint dataPoint) {
    //    AccumCategoricalBuilder accumStoreCoreCategorical = new AccumCategoricalBuilder(accum);
    //    accumStoreCoreCategorical.setDOW(createNumData(time.toDateTime().dayOfWeek().get()));
    //    accumStoreCoreCategorical.setDOM(createNumData(time.toDateTime().dayOfMonth().get()));
    //    accumStoreCoreCategorical.setYY(createNumData(time.toDateTime().year().get()));

    return null;
  }
}
