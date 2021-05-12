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
package com.google.dataflow.sample.timeseriesflow.metrics.complex.rsi;

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;
import org.apache.beam.sdk.annotations.Experimental;

/** Builder for the {@link ComputeSumUpSumDownWithCount} type 2 computation data store */
@Experimental
public class AccumSumUpDownBuilder extends AccumCoreMetadataBuilder {
  public AccumSumUpDownBuilder(TSAccum tsAccum) {
    super(tsAccum);
  }

  Data getSumUp() {
    return getValueOrNull(FsiTechnicalIndicators.SUM_UP_MOVEMENT.name());
  }

  Data getSumDown() {
    return getValueOrNull(FsiTechnicalIndicators.SUM_DOWN_MOVEMENT.name());
  }

  Data getMovementCount() {
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
