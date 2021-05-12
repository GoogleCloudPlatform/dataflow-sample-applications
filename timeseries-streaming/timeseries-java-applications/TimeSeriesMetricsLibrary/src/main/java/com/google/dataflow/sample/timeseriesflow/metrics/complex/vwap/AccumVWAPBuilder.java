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

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data.DataPointCase;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;

public class AccumVWAPBuilder extends AccumCoreMetadataBuilder {

  static final String VWAP = "VWAP";
  static final String PRICE_VOLUME = "PRICE_VOLUME";
  static final String QTY = "QTY";

  // The Price Volume is == to the SUM for the TSAccum
  static final String _PRICE_VOLUME = PRICE_VOLUME + "-" + Indicators.SUM.name();

  // The QTY is == the Sum of all Qty used to generate this TSAccum
  static final String _QTY = QTY + "-" + Indicators.SUM.name();

  public AccumVWAPBuilder(TSAccum tsAccum) {
    super(tsAccum);
  }

  public Data getPriceVolume() {
    return getValueOrNull(_PRICE_VOLUME);
  }

  public Data getQuantity() {
    return getValueOrNull(_QTY);
  }

  public Data getPriceIsHB() {
    return getValueOrZero(PRICE_VOLUME + "-" + "hb", DataPointCase.INT_VAL);
  }

  public Data getQuantityIsHB() {
    return getValueOrNull(QTY + "-" + "hb");
  }

  public void setVWAP(Data data) {
    setValue(VWAP, data);
  }
}
