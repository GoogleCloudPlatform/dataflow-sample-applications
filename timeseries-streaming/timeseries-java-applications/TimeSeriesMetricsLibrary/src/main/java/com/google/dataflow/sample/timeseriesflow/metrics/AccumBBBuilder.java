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
package com.google.dataflow.sample.timeseriesflow.metrics;

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;
import org.apache.beam.sdk.annotations.Experimental;

/** Builder for the {@link BB} type 2 computation data store */
@Experimental
class AccumBBBuilder extends AccumCoreMetadataBuilder {
  public AccumBBBuilder(TSAccum tsAccum) {
    super(tsAccum);
  }

  public Data getMovementCount() {
    return getValueOrNull(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name());
  }

  public Data getMidBandSMA() {
    return getValueOrNull(FsiTechnicalIndicators.BB_MIDDLE_BAND_SMA.name());
  }

  public Data getUpperBandSMA() {
    return getValueOrNull(FsiTechnicalIndicators.BB_UPPER_BAND_SMA.name());
  }

  public Data getBottomBandSMA() {
    return getValueOrNull(FsiTechnicalIndicators.BB_BOTTOM_BAND_SMA.name());
  }

  public Data getMidBandEMA() {
    return getValueOrNull(FsiTechnicalIndicators.BB_MIDDLE_BAND_EMA.name());
  }

  public Data getUpperBandEMA() {
    return getValueOrNull(FsiTechnicalIndicators.BB_UPPER_BAND_EMA.name());
  }

  public Data getBottomBandEMA() {
    return getValueOrNull(FsiTechnicalIndicators.BB_BOTTOM_BAND_EMA.name());
  }

  public AccumBBBuilder setMovementCount(Data data) {
    setValue(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name(), data);
    return this;
  }

  public AccumBBBuilder setMidBandSMA(Data data) {
    setValue(FsiTechnicalIndicators.BB_MIDDLE_BAND_SMA.name(), data);
    return this;
  }

  public AccumBBBuilder setUpperBandSMA(Data data) {
    setValue(FsiTechnicalIndicators.BB_UPPER_BAND_SMA.name(), data);
    return this;
  }

  public AccumBBBuilder setBottomBandSMA(Data data) {
    setValue(FsiTechnicalIndicators.BB_BOTTOM_BAND_SMA.name(), data);
    return this;
  }

  public AccumBBBuilder setMidBandEMA(Data data) {
    setValue(FsiTechnicalIndicators.BB_MIDDLE_BAND_EMA.name(), data);
    return this;
  }

  public AccumBBBuilder setUpperBandEMA(Data data) {
    setValue(FsiTechnicalIndicators.BB_UPPER_BAND_EMA.name(), data);
    return this;
  }

  public AccumBBBuilder setBottomBandEMA(Data data) {
    setValue(FsiTechnicalIndicators.BB_BOTTOM_BAND_EMA.name(), data);
    return this;
  }
}
