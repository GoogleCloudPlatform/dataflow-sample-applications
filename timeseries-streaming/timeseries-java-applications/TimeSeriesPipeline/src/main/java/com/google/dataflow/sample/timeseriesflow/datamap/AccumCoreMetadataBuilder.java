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
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
/**
 * Accum Core Builder, dealing with common metadata , timestamps, first and last value and the
 * count.
 */
public class AccumCoreMetadataBuilder extends AccumBuilder {
  public AccumCoreMetadataBuilder(TSAccum tsAccum) {
    super(tsAccum);
  }

  public Timestamp getFirstTimestampOrZero() {
    return getTimestampValueOrZero(Indicators.FIRST_TIMESTAMP.name());
  }

  public Timestamp getLastTimestampOrZero() {
    return getTimestampValueOrZero(Indicators.LAST_TIMESTAMP.name());
  }

  public void setFirstTimestamp(Timestamp timestamp) {
    setTimestampValue(Indicators.FIRST_TIMESTAMP.name(), timestamp);
  }

  public void setLastTimestamp(Timestamp timestamp) {
    setTimestampValue(Indicators.LAST_TIMESTAMP.name(), timestamp);
  }

  public void setFirstTimestampIfSmaller(Timestamp timestamp) {
    setTimestampValue(
        Indicators.FIRST_TIMESTAMP.name(),
        CommonUtils.getMinTimeStamp(
            getTimestampValueOrNull(Indicators.FIRST_TIMESTAMP.name()), timestamp));
  }

  public void setFirst(Data data) {
    Preconditions.checkNotNull(data);
    setValue(Indicators.FIRST.name(), data);
  }

  public void setLast(Data data) {
    Preconditions.checkNotNull(data);
    setValue(Indicators.LAST.name(), data);
  }

  public Long getCountValueOrZero() {
    Data count =
        Optional.ofNullable(getValueOrNull(Indicators.DATA_POINT_COUNT.name()))
            .orElse(CommonUtils.createNumData(0L));
    return count.getLongVal();
  }

  public Data getCountOrNull() {
    return getValueOrNull(Indicators.DATA_POINT_COUNT.name());
  }

  public void setCount(Data data) {
    Preconditions.checkNotNull(data);
    setValue(Indicators.DATA_POINT_COUNT.name(), data);
  }

  public void setCountValue(Long value) {
    Preconditions.checkNotNull(value);
    setValue(Indicators.DATA_POINT_COUNT.name(), CommonUtils.createNumData(value));
  }

  public Data getFirstOrNull() {
    return getValueOrNull(Indicators.FIRST.name());
  }

  public Data getLastOrNull() {
    return getValueOrNull(Indicators.LAST.name());
  }
}
