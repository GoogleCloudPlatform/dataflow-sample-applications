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

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * Class that merges {@link TSAccum} in a single Window together. Will throw error if the same
 * properties exists with two {@link TSAccum} in the same Key-Window space.
 *
 * <p>In order to match the Type 1 and Type 2 calculations the Type FixedWindow size must == the
 * Type 2 sliding window offset size.
 */
@AutoValue
@Experimental
public abstract class MergeAllTypeCompsInSameKeyWindow
    extends PTransform<PCollectionList<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccum>>> {

  public abstract Window<KV<TSKey, TSAccum>> mergeWindow();

  public abstract Builder builder();

  public static MergeAllTypeCompsInSameKeyWindow withMergeWindow(
      Window<KV<TSKey, TSAccum>> mergeWindow) {
    return new AutoValue_MergeAllTypeCompsInSameKeyWindow.Builder()
        .setMergeWindow(mergeWindow)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setMergeWindow(Window<KV<TSKey, TSAccum>> value);

    public abstract MergeAllTypeCompsInSameKeyWindow build();
  }

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollectionList<KV<TSKey, TSAccum>> input) {

    List<PCollection<KV<TSKey, TSAccum>>> collections = input.getAll();
    List<PCollection<KV<TSKey, TSAccum>>> windowedCollections = new ArrayList<>();

    for (PCollection<KV<TSKey, TSAccum>> coll : collections) {

      windowedCollections.add(coll.apply(coll.getName(), mergeWindow()));
    }

    return PCollectionList.of(windowedCollections)
        .apply(Flatten.pCollections())
        .apply(GroupByKey.create())
        .apply(ParDo.of(new MergeAccum()));
  }

  private static class MergeAccum extends DoFn<KV<TSKey, Iterable<TSAccum>>, KV<TSKey, TSAccum>> {

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
          if (merged.getDataStoreMap().containsKey(key)) {
            Data existingData = merged.getDataStoreMap().get(key);
            if (!existingData.equals(tsAccum.getDataStoreOrThrow(key))) {
              throw new IllegalStateException(
                  String.format(
                      "%s already seen in this Key-Window, however this value is different than the one seen, this suggests there is a namespace collision within the type 1 or type 2 generators.",
                      key));
            }
          }
          dataMap.put(key, tsAccum.getDataStoreOrThrow(key));
        }

        merged.putAllDataStore(dataMap);
        Optional.ofNullable(upperWindowBoundary).ifPresent(merged::setUpperWindowBoundary);
        Optional.ofNullable(lowerWindowBoundary).ifPresent(merged::setLowerWindowBoundary);
        merged.setHasAGapFillMessage(isGapFillValue);
      }

      /*
      Check if the merged value contains type 1 computations, if not then we do not output as this will result in TSAccum without some features.
      This can happen as the type 2 window is a sliding one , which will slide forward until the last known type 1 computation passes the tail of the sliding window.
      */

      if (merged
              .getDataStoreOrDefault(
                  Indicators.DATA_POINT_COUNT.name(), Data.newBuilder().setLongVal(0).build())
              .getLongVal()
          > 0) {
        pc.output(KV.of(pc.element().getKey(), merged.setKey(pc.element().getKey()).build()));
      }
    }
  }
}
