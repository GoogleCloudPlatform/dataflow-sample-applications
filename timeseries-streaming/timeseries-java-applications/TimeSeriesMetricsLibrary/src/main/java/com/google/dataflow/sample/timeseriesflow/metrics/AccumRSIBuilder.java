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
import org.apache.beam.sdk.annotations.Experimental;

/** Builder for the {@link RSI} type 2 computation data store */
@Experimental
public class AccumRSIBuilder extends AccumSumUpDownBuilder {
  public AccumRSIBuilder(TSAccum tsAccum) {
    super(tsAccum);
  }

  public Data getABSMovingAverageGain() {
    return getValueOrNull(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_GAIN.name());
  }

  public Data getABSMovingAverageLoss() {
    return getValueOrNull(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_LOSS.name());
  }

  public Data getRelativeStrength() {
    return getValueOrNull(FsiTechnicalIndicators.RELATIVE_STRENGTH.name());
  }

  public Data getRelativeStrengthIndicator() {
    return getValueOrNull(FsiTechnicalIndicators.RELATIVE_STRENGTH_INDICATOR.name());
  }

  public AccumRSIBuilder setABSMovingAverageGain(Data data) {
    setValue(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_GAIN.name(), data);
    return this;
  }

  public AccumRSIBuilder setABSMovingAverageLoss(Data data) {
    setValue(FsiTechnicalIndicators.ABS_MOVING_AVERAGE_LOSS.name(), data);
    return this;
  }

  public AccumRSIBuilder setRelativeStrength(Data data) {
    setValue(FsiTechnicalIndicators.RELATIVE_STRENGTH.name(), data);
    return this;
  }

  public AccumRSIBuilder setRelativeStrengthIndicator(Data data) {
    setValue(FsiTechnicalIndicators.RELATIVE_STRENGTH_INDICATOR.name(), data);
    return this;
  }
}
