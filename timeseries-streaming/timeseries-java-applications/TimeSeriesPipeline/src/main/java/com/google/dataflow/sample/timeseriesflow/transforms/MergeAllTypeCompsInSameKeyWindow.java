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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSBaseCombiner;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Class that merges {@link TSAccum} in a single Window together. Will throw error if the same
 * properties exists with two {@link TSAccum} in the same Key-Window space.
 *
 * <p>In order to match the Type 1 and Type 2 calculations the Type FixedWindow size must == the
 * Type 2 sliding window offset size.
 *
 * <p>Some computations generate extra metrics which may not be required in the final output. These
 * can be excluded by setting the {@link
 * MergeAllTypeCompsInSameKeyWindow.Builder#setMetricExcludeList(List)}
 */
@AutoValue
@Experimental
public abstract class MergeAllTypeCompsInSameKeyWindow
    extends PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccum>>> {

  @Nullable
  public abstract List<String> metricExcludeList();

  public abstract Builder builder();

  public static MergeAllTypeCompsInSameKeyWindow create() {
    return new AutoValue_MergeAllTypeCompsInSameKeyWindow.Builder().build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setMetricExcludeList(List<String> excludeList);

    public abstract MergeAllTypeCompsInSameKeyWindow build();
  }

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccum>> input) {

    return input
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new MergeTSAccum(
                    Optional.ofNullable(metricExcludeList()).orElse(ImmutableList.of()))))
        /*
        Check if the merged value contains type 1 computations, if not then we do not output as this will result in TSAccum without some features.
        This can happen as the type 2 window is a sliding one , which will slide forward until the last known type 1 computation passes the tail of the sliding window.
        */
        .apply(
            Filter.by(
                x ->
                    x.getValue()
                        .getMetadataOrDefault(TSBaseCombiner._BASE_COMBINER, "f")
                        .equals("t")));
  }
}
