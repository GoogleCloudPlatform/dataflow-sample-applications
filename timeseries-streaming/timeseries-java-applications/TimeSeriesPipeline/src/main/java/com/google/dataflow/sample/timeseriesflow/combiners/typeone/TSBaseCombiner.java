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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreMetadataBuilder;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.Iterator;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * Creates a accumulator for all data types, which deals with the common metadata like time. This
 * base combine can be used to build out type specific aggregator's for a given Univariate time
 * series.
 */
@Experimental
public abstract class TSBaseCombiner extends CombineFn<TSDataPoint, TSAccum, TSAccum> {

  // Indicator stored in the internal proto.
  public static final String _BASE_COMBINER = "_BC";

  @Override
  public TSAccum createAccumulator() {
    return TimeSeriesData.TSAccum.newBuilder().setIsAllGapFillMessages(true).build();
  }

  /*
  Set common values, first , last and count for the aggregation
   */
  @Override
  public TimeSeriesData.TSAccum addInput(
      TimeSeriesData.TSAccum accumulator, TimeSeriesData.TSDataPoint dataPoint) {

    AccumCoreNumericBuilder accumStore = new AccumCoreNumericBuilder(accumulator);

    // Update the first timestamp metadata value seen in this accum

    Timestamp firstTimestamp =
        CommonUtils.getMinTimeStamp(accumStore.getFirstTimestampOrZero(), dataPoint.getTimestamp());

    accumStore.setFirstTimestamp(firstTimestamp);

    // If our DataPoints timestamp is == min then use this DataPoint
    // TODO document that this is non-deterministic
    if (dataPoint.getTimestamp() == firstTimestamp) {
      accumStore.setFirst(dataPoint.getData());
    }

    Timestamp lastTimestamp =
        CommonUtils.getMaxTimeStamp(accumStore.getLastTimestampOrZero(), dataPoint.getTimestamp());

    accumStore.setLastTimestamp(lastTimestamp);

    // If our DataPoints timestamp is == max then use this DataPoint
    // TODO document that this is non-deterministic
    if (dataPoint.getTimestamp() == lastTimestamp) {
      accumStore.setLast(dataPoint.getData());
    }

    // Increment the total count if this is not a HB message
    if (!dataPoint.getIsAGapFillMessage()) {
      accumStore.setCountValue(accumStore.getCountValueOrZero() + 1);
    }

    TSAccum accum = addTypeSpecificInput(accumStore.build(), dataPoint);

    // If the dataPoint has a HB value set, which indicates its a gapfill value, then set it in the
    // accum to indicate a HB was seen.
    if (dataPoint.getIsAGapFillMessage()) {
      return accum.toBuilder().setKey(dataPoint.getKey()).setHasAGapFillMessage(true).build();
    }

    // As no HB value was set we can mark that at least one value in the TSAccum computation was not
    // a gap fill value
    return accum.toBuilder().setKey(dataPoint.getKey()).setIsAllGapFillMessages(false).build();
  }

  @Override
  public TimeSeriesData.TSAccum mergeAccumulators(Iterable<TSAccum> accumulators) {

    Iterator<TSAccum> iterator = accumulators.iterator();

    TimeSeriesData.TSAccum current = iterator.next();

    while (iterator.hasNext()) {
      TimeSeriesData.TSAccum next = iterator.next();
      current = mergeRightAccum(current, next);
    }

    return current;
  }

  @Override
  public TimeSeriesData.TSAccum extractOutput(TimeSeriesData.TSAccum accum) {
    // Check HB condition, if HB is set and count == 1 then this is a HB accum
    AccumCoreMetadataBuilder accumBuilder = new AccumCoreMetadataBuilder(accum);

    TSAccum.Builder output = accum.toBuilder();

    if (accumBuilder.getCountValueOrZero() > 1) {
      output.setHasAGapFillMessage(false);
    }
    output.putMetadata(_BASE_COMBINER, "t");

    return output.build();
  }

  private TSAccum mergeRightAccum(TSAccum leftAccum, TSAccum rightAccum) {

    if (!leftAccum.hasKey()) {
      leftAccum = leftAccum.toBuilder().setKey(rightAccum.getKey()).build();
    }

    // If the right accum has seen a gapfill message then set the left item as well.
    if (rightAccum.getHasAGapFillMessage()) {
      leftAccum = leftAccum.toBuilder().setHasAGapFillMessage(true).build();
    }

    if (!rightAccum.getIsAllGapFillMessages()) {
      leftAccum = leftAccum.toBuilder().setIsAllGapFillMessages(false).build();
    }

    AccumCoreMetadataBuilder leftAccumMetadata = new AccumCoreMetadataBuilder(leftAccum);
    AccumCoreMetadataBuilder rightAccumMetadata = new AccumCoreMetadataBuilder(rightAccum);

    leftAccumMetadata.setCountValue(
        leftAccumMetadata.getCountValueOrZero() + rightAccumMetadata.getCountValueOrZero());

    Timestamp firstTimeStamp =
        CommonUtils.getMinTimeStamp(
            leftAccumMetadata.getFirstTimestampOrZero(),
            rightAccumMetadata.getFirstTimestampOrZero());

    leftAccumMetadata.setFirstTimestamp(firstTimeStamp);

    if (Timestamps.compare(rightAccumMetadata.getFirstTimestampOrZero(), firstTimeStamp) == 0) {
      leftAccumMetadata.setFirst(rightAccumMetadata.getFirstOrNull());
    }

    Timestamp lastTimestamp =
        CommonUtils.getMaxTimeStamp(
            leftAccumMetadata.getLastTimestampOrZero(),
            rightAccumMetadata.getLastTimestampOrZero());

    leftAccumMetadata.setLastTimestamp(lastTimestamp);

    if (Timestamps.compare(rightAccumMetadata.getLastTimestampOrZero(), lastTimestamp) == 0) {
      leftAccumMetadata.setLast(rightAccumMetadata.getLastOrNull());
    }

    return mergeTypedDataAccum(leftAccumMetadata.build(), rightAccum);
  }

  public abstract TSAccum mergeTypedDataAccum(TSAccum a, TSAccum b);

  public abstract TimeSeriesData.TSAccum addTypeSpecificInput(
      TimeSeriesData.TSAccum accumulator, TSDataPoint dataPoint);
}
