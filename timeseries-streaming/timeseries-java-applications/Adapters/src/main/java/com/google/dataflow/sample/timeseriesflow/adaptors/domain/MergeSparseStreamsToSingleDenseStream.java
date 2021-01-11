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
package com.google.dataflow.sample.timeseriesflow.adaptors.domain;

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given n streams, this will create a {@link Iterable<TSDataPoint>} for each {@link
 * MergeSparseStreamsToSingleDenseStream#getDownSampleWindowLength()} }.
 *
 * <p>Gap filling will be used once a key has been seen for the first time until {@link
 * MergeSparseStreamsToSingleDenseStream#getAbsoluteStopTime() } or {@link
 * MergeSparseStreamsToSingleDenseStream#getTimeToLive()} }
 */
@AutoValue
@Experimental
public abstract class MergeSparseStreamsToSingleDenseStream
    extends PTransform<PCollection<TSDataPoint>, PCollection<Iterable<TSDataPoint>>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(MergeSparseStreamsToSingleDenseStream.class);

  abstract Set<TSKey> getIndexList();

  abstract Duration getDownSampleWindowLength();

  abstract @Nullable Instant getAbsoluteStopTime();

  abstract @Nullable Duration getTimeToLive();

  abstract TSKey getNewIndexKey();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setIndexList(Set<TSKey> newIndexList);

    public abstract Builder setDownSampleWindowLength(Duration newDownSampleWindowLength);

    public abstract Builder setAbsoluteStopTime(Instant newAbsoluteStopTime);

    public abstract Builder setTimeToLive(Duration newTimeToLive);

    public abstract Builder setNewIndexKey(TSKey newIndexKey);

    public abstract MergeSparseStreamsToSingleDenseStream build();
  }

  public static Builder builder() {
    return new AutoValue_MergeSparseStreamsToSingleDenseStream.Builder();
  }

  @Override
  public PCollection<Iterable<TSDataPoint>> expand(PCollection<TSDataPoint> input) {

    PerfectRectangles perfectRectangles = null;

    if (getTimeToLive() == null) {
      perfectRectangles =
          PerfectRectangles.withWindowAndAbsoluteStop(
                  getDownSampleWindowLength(), getAbsoluteStopTime())
              .enablePreviousValueFill();
    }

    if (getAbsoluteStopTime() == null) {

      perfectRectangles =
          PerfectRectangles.withWindowAndTTLDuration(getDownSampleWindowLength(), getTimeToLive())
              .enablePreviousValueFill();
    }

    Preconditions.checkNotNull(perfectRectangles);

    PCollection<KV<String, TSDataPoint>> valuesInWindow =
        input
            .apply("FilterIndexes", Filter.by(x -> getIndexList().contains(x.getKey())))
            .apply(WithKeys.of(x -> x.getKey()))
            .setCoder(CommonUtils.getKvTSDataPointCoder())
            .apply(perfectRectangles)
            .apply(Reify.windowsInValue())
            .apply(
                "CreateWindowKeys",
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.strings(), TypeDescriptor.of(TSDataPoint.class)))
                    .via(x -> KV.of(x.getValue().getWindow().toString(), x.getValue().getValue())));

    return valuesInWindow.apply(GroupByKey.create()).apply(Values.create());
  }
}
