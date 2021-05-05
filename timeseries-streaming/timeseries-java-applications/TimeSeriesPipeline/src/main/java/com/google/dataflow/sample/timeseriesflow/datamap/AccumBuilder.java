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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data.DataPointCase;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum.Builder;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
/** Base Builder for the data stored in a {@link TSAccum} data store. */
public abstract class AccumBuilder {

  private Builder tsAccum;

  public AccumBuilder(TSAccum tsAccum) {
    this.tsAccum = tsAccum.toBuilder();
  }

  public Data getValueOrNull(String key) {
    return tsAccum.getDataStoreOrDefault(key, null);
  }

  public Data getValueOrZero(String key, DataPointCase type) {
    return tsAccum.getDataStoreOrDefault(key, CommonUtils.createZeroOfType(type));
  }

  public void setValue(String key, Data value) {
    Preconditions.checkNotNull(key, value);
    tsAccum.putDataStore(key, value);
  }

  public void removeValue(String key) {
    tsAccum.removeDataStore(key);
  }

  public Timestamp getTimestampValueOrZero(String key) {
    return Timestamps.fromMillis(
        tsAccum.getDataStoreOrDefault(key, CommonUtils.createNumData(0L)).getLongVal());
  }

  public Timestamp getTimestampValueOrNull(String key) {

    Data data = tsAccum.getDataStoreOrDefault(key, null);
    if (data == null) {
      return null;
    }

    return Timestamps.fromMillis(data.getLongVal());
  }

  public void setTimestampValue(String key, Timestamp timestamp) {
    tsAccum.putDataStore(key, CommonUtils.createNumData(Timestamps.toMillis(timestamp)));
  }

  public TSAccum build() {
    return tsAccum.build();
  }

  public boolean dataIsSet(Data data) {
    if (data.getDataPointCase().equals(DataPointCase.DATAPOINT_NOT_SET)) {
      return false;
    }
    return true;
  }
}
