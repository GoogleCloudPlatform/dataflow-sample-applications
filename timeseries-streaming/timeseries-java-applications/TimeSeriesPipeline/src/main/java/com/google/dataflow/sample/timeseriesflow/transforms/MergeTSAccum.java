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
package com.google.dataflow.sample.timeseriesflow.transforms;

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.protobuf.util.Timestamps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Merges TSAccum which are in the same window. Carries out validation on the data as the merge is
 * based on assumptions.
 *
 * <p>1- For every metric in a single window, only one value should exist. If duplicate keys the
 * value must be equal. 2- Only TSAccum in the same window will be present in the same Iterable.
 */
public class MergeTSAccum extends DoFn<KV<TSKey, Iterable<TSAccum>>, KV<TSKey, TSAccum>> {

  public List<String> metricExcludeList;

  public MergeTSAccum() {
    metricExcludeList = ImmutableList.of();
  }

  public MergeTSAccum(List<String> metricExcludeList) {
    this.metricExcludeList = metricExcludeList;
  }

  @ProcessElement
  public void process(ProcessContext pc, IntervalWindow w) {
    TSAccum.Builder merged = TSAccum.newBuilder();

    Map<String, Data> dataMap = new HashMap<>();

    com.google.protobuf.Timestamp upperWindowBoundary = null;
    com.google.protobuf.Timestamp lowerWindowBoundary = null;
    boolean isGapFillValue = false;

    for (TSAccum tsAccum : pc.element().getValue()) {

      if (tsAccum.getHasAGapFillMessage()) {
        isGapFillValue = true;
      }

      if (upperWindowBoundary != null && tsAccum.hasUpperWindowBoundary()) {
        if (Timestamps.compare(upperWindowBoundary, tsAccum.getUpperWindowBoundary()) != 0) {
          throw new IllegalStateException(
              String.format(
                  "Different time boundary accums are being merged! %s with %s accums are %s and %s",
                  Timestamps.toString(upperWindowBoundary),
                  Timestamps.toString(tsAccum.getUpperWindowBoundary()),
                  merged,
                  tsAccum));
        }
      }

      if (tsAccum.hasUpperWindowBoundary()) {
        upperWindowBoundary = tsAccum.getUpperWindowBoundary();
      }

      if (lowerWindowBoundary != null && tsAccum.hasLowerWindowBoundary()) {
        if (Timestamps.compare(lowerWindowBoundary, tsAccum.getLowerWindowBoundary()) != 0) {
          throw new IllegalStateException(
              String.format(
                  "Different time boundary accums are being merged! %s with %s accums are %s and %s",
                  Timestamps.toString(lowerWindowBoundary),
                  Timestamps.toString((tsAccum.getLowerWindowBoundary())),
                  merged,
                  tsAccum));
        }
      }

      if (tsAccum.hasLowerWindowBoundary()) {
        lowerWindowBoundary = tsAccum.getLowerWindowBoundary();
      }

      // In this version of the library we can have multiple values of Type 2 sub computations
      // appear in the Accum.
      // This is because the library does not contain an optimizer for its computations to avoid
      // redundancy yet.
      // However the same value for the same window must be equal, otherwise there is a name space
      // clash which is a bug.

      for (String key : tsAccum.getDataStoreMap().keySet()) {
        // Dont include keys if they are in the metricExcludeList
        if (!metricExcludeList.contains(key)) {
          if (merged.getDataStoreMap().containsKey(key)) {
            Data existingData = merged.getDataStoreMap().get(key);
            if (!existingData
                .getDataPointCase()
                .equals(tsAccum.getDataStoreOrThrow(key).getDataPointCase())) {
              throw new IllegalStateException(
                  String.format(
                      "%s already seen in this Key-Window %s-%s, however this value has different type the one seen, this suggests incorrect creation of the TSDataPoint when the data from source was being processed. Normally this can happen if the same Minor Key value is used for Data points with different data types.",
                      key, tsAccum.getKey(), tsAccum.getLowerWindowBoundary()));
            }

            if (!existingData.equals(tsAccum.getDataStoreOrThrow(key))) {
              throw new IllegalStateException(
                  String.format(
                      "%s already seen in this Key-Window %s-%s, however this value is different than the one seen, this suggests there is a namespace collision within the type 1 or type 2 generators.",
                      key, tsAccum.getKey(), tsAccum.getLowerWindowBoundary()));
            }
          }
          dataMap.put(key, tsAccum.getDataStoreOrThrow(key));
        }
      }

      merged.putAllDataStore(dataMap);
      Optional.ofNullable(upperWindowBoundary).ifPresent(merged::setUpperWindowBoundary);
      Optional.ofNullable(lowerWindowBoundary).ifPresent(merged::setLowerWindowBoundary);
      merged.setHasAGapFillMessage(isGapFillValue);
      // TODO this will overwrite values, change to create when values are not equal.
      merged.putAllMetadata(tsAccum.getMetadataMap());
    }

    if (merged
            .getDataStoreOrDefault(
                Indicators.DATA_POINT_COUNT.name(), Data.newBuilder().setLongVal(0).build())
            .getLongVal()
        < 1) {
      merged.setIsAllGapFillMessages(true);
    }

    pc.output(KV.of(pc.element().getKey(), merged.setKey(pc.element().getKey()).build()));
  }
}
