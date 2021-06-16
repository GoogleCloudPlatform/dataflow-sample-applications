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
package com.google.dataflow.sample.timeseriesflow.common;

import static com.google.protobuf.util.Timestamps.toMillis;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data.DataPointCase;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.options.TFXOptions;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

@Experimental
/** Utilities. */
public class CommonUtils {

  /** Return the larger of the timestamps returns {@link Timestamp} */
  public static Timestamp getMaxTimeStamp(Timestamp a, Timestamp b) {
    return (Timestamps.comparator().compare(a, b) > 0) ? a : b;
  }

  /** Return the smaller of the timestamps returns {@link Timestamp} */
  public static Timestamp getMinTimeStamp(Timestamp a, Timestamp b) {

    if (toMillis(a) == 0 && toMillis(b) > 0) {
      return b;
    }

    if (toMillis(b) == 0) {
      return a;
    }

    return (Timestamps.comparator().compare(a, b) < 0) ? a : b;
  }

  /** merges the Metadata from TSDataPoints into a map */
  public static Map<String, String> mergeDataPoints(List<TSDataPoint> datapoints) {

    Map<String, String> mergedMap = new HashMap<>();
    for (TSDataPoint dataPoint : datapoints) {
      mergedMap.putAll(dataPoint.getMetadataMap());
    }
    return mergedMap;
  }

  /** returns a {@link Data} given a {@link Number} */
  public static <T extends Number> Data createNumData(T data) {

    if (data instanceof Long) {
      return Data.newBuilder().setLongVal((Long) data).build();
    }

    if (data instanceof Double) {
      return Data.newBuilder().setDoubleVal((Double) data).build();
    }

    if (data instanceof Integer) {
      return Data.newBuilder().setIntVal((Integer) data).build();
    }

    if (data instanceof Float) {
      return Data.newBuilder().setFloatVal((Float) data).build();
    }

    throw new IllegalArgumentException("Must be Double, Long, Integer or Float");
  }

  /** returns a {@link Data} given a string representing a number */
  public static Data createNumAsStringData(String data) {

    return Data.newBuilder().setNumAsString(data).build();
  }

  /** Returns a {@link DataPointCase} with a value of 0. Support long, double, float, int only. */
  public static Data createZeroOfType(DataPointCase dataType) {
    Preconditions.checkArgument(
        !(dataType.equals(DataPointCase.DATAPOINT_NOT_SET)
            || dataType.equals(DataPointCase.CATEGORICAL_VAL)));

    Data data = null;

    switch (dataType) {
      case LONG_VAL:
        data = createNumData(0L);
        break;
      case DOUBLE_VAL:
        data = createNumData(0D);
        break;
      case FLOAT_VAL:
        data = createNumData(0F);
        break;
      case INT_VAL:
        data = createNumData(0);
        break;
      case NUM_AS_STRING:
        data = createNumAsStringData(BigDecimal.ZERO.toString());
        break;
    }

    return data;
  }

  /** Returns true if a {@link Data} has a value */
  public static boolean hasData(Data data) {
    if (data == null) {
      return false;
    }
    return data.getDataPointCase() != DataPointCase.DATAPOINT_NOT_SET;
  }

  /**
   * Add two {@link Data} together.
   *
   * @param dataA
   * @param dataB
   * @return Data
   */
  public static Data sumNumericDataNullAsZero(Data dataA, Data dataB) {

    Preconditions.checkArgument(
        (hasData(dataA) || hasData(dataB)), "Both data points can not be null.");

    DataPointCase type = (hasData(dataA)) ? dataA.getDataPointCase() : dataB.getDataPointCase();

    if (!hasData(dataA)) {
      dataA = createZeroOfType(type);
    }

    if (!hasData(dataB)) {
      dataB = createZeroOfType(type);
    }

    switch (type) {
      case INT_VAL:
        return Data.newBuilder().setIntVal(dataA.getIntVal() + dataB.getIntVal()).build();
      case FLOAT_VAL:
        return Data.newBuilder().setFloatVal(dataA.getFloatVal() + dataB.getFloatVal()).build();
      case DOUBLE_VAL:
        return Data.newBuilder().setDoubleVal(dataA.getDoubleVal() + dataB.getDoubleVal()).build();
      case LONG_VAL:
        return Data.newBuilder().setLongVal(dataA.getLongVal() + dataB.getLongVal()).build();
      case NUM_AS_STRING:
        return Data.newBuilder()
            .setNumAsString(
                new BigDecimal(dataA.getNumAsString())
                    .add(new BigDecimal(dataB.getNumAsString()))
                    .toString())
            .build();
    }

    throw new IllegalArgumentException("Must be Double, Long, Integer, Float, or Num as String");
  }

  public static Schema tsAccumSequenceSchema() {
    return new ProtoMessageSchema().schemaFor(TypeDescriptor.of(TSAccumSequence.class));
  }

  public static Coder<KV<TSKey, TSAccum>> getKvTSAccumCoder() {
    return KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSAccum.class));
  }

  public static Coder<KV<TSKey, TSDataPoint>> getKvTSDataPointCoder() {
    return KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class));
  }

  public static Coder<KV<TSKey, TSAccumSequence>> getKvTSAccumSequenceCoder() {
    return KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSAccumSequence.class));
  }

  public static TypeDescriptor<KV<TSKey, TSAccum>> getKvTSAccumTypedescritors() {
    return TypeDescriptors.kvs(TypeDescriptor.of(TSKey.class), TypeDescriptor.of(TSAccum.class));
  }

  public static TypeDescriptor<KV<TSKey, TSAccumSequence>> getKvTSAccumSequenceTypedescritors() {
    return TypeDescriptors.kvs(
        TypeDescriptor.of(TSKey.class), TypeDescriptor.of(TSAccumSequence.class));
  }
  /**
   * The output sequence length is derived from the Type 1 fixed length value divided by the desired
   * output sequence in seconds. The sequence length must be a multiple of the type 1 fixed value.
   *
   * @param tfxOptions
   * @return {@link Integer}
   */
  public static Integer getNumOfSequenceTimesteps(TFXOptions tfxOptions) {
    Preconditions.checkNotNull(tfxOptions);
    Integer type1length = tfxOptions.getTypeOneComputationsLengthInSecs();
    Integer desiredSequenceLength = tfxOptions.getOutputTimestepLengthInSecs();

    Preconditions.checkNotNull(type1length);
    Preconditions.checkNotNull(desiredSequenceLength);

    if (type1length < 1 || desiredSequenceLength < 1) {
      throw new IllegalArgumentException(
          "The type 1 length and desired sequence length must be greater than 0");
    }

    if (desiredSequenceLength % type1length != 0) {
      throw new IllegalArgumentException(
          "The sequence length must be a multiple of the type 1 length");
    }
    return desiredSequenceLength / type1length;
  }
}
