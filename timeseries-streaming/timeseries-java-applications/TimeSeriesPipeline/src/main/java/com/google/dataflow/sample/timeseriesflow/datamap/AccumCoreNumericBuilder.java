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
package com.google.dataflow.sample.timeseriesflow.datamap;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
/** Accum Numeric Builder, dealing with common numeric aggregations Sum, Min, Max, First, Last. */
public class AccumCoreNumericBuilder extends AccumCoreMetadataBuilder {

  public AccumCoreNumericBuilder(TSAccum tsAccum) {
    super(tsAccum);
  }

  public Data getSumOrNull() {
    return getValueOrNull(Indicators.SUM.name());
  }

  public Data getMaxOrNull() {
    return getValueOrNull(Indicators.MAX.name());
  }

  public Data getMinOrNull() {
    return getValueOrNull(Indicators.MIN.name());
  }

  public void setSum(Data data) {
    Preconditions.checkNotNull(data);
    setValue(Indicators.SUM.name(), data);
  }

  public void setMax(Data data) {

    Preconditions.checkNotNull(data);
    setValue(Indicators.MAX.name(), data);
  }

  public void setMin(Data data) {
    Preconditions.checkNotNull(data);
    setValue(Indicators.MIN.name(), data);
  }
}
