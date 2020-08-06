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

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
/** Builder for the as yet un- implemented Categorical values. */
public class AccumCategoricalBuilder extends AccumCoreMetadataBuilder {

  public AccumCategoricalBuilder(TSAccum tsAccum) {
    super(tsAccum);
  }

  public Data getDOWOrNull() {
    return getValueOrNull(Indicators.DOW.name());
  }

  public Data getDOMOrNull() {
    return getValueOrNull(Indicators.DOM.name());
  }

  public Data getYYOrNull() {
    return getValueOrNull(Indicators.YY.name());
  }

  public void setDOW(Data data) {
    setValue(Indicators.DOW.name(), data);
  }

  public void setDOM(Data data) {
    setValue(Indicators.DOM.name(), data);
  }

  public void setYY(Data data) {
    setValue(Indicators.YY.name(), data);
  }
}
