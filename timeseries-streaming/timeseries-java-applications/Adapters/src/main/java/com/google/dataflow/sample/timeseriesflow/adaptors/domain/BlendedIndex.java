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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.protobuf.util.Timestamps;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A domain adaptor that consumes a stream of 1 or more values and produces a new value per window
 * based on ratios that are provided.
 *
 * <p>The values are expected to have come from the Type 1 computations with the last value per
 * window used as the value to create the index from.
 *
 * <p>Ratios must add up to 1
 *
 * <p>example:
 *
 * <p>[ timeWindow1, timeWindow2, timeWindow3 ]
 *
 * <p>Key A [ 10, No Data, 12 ] Key B [ 100, No Data, 110]
 *
 * <p>Ratios [0.8,0.2]
 *
 * <p>Output [ ((10*.8) + (100*.2)),((10*.8) + (100*.2)), ((20*.8) + (110*.2)) ]
 */
@AutoValue
@Experimental
public abstract class BlendedIndex
    extends PTransform<PCollection<TSDataPoint>, PCollection<TSDataPoint>> {

  private static final Logger LOG = LoggerFactory.getLogger(BlendedIndex.class);

  abstract PCollectionView<Map<TSKey, Double>> getRatioList();

  abstract Set<TSKey> getIndexList();

  abstract Duration getDownSampleWindowLength();

  abstract @Nullable Instant getAbsoluteStopTime();

  abstract @Nullable Duration getTimeToLive();

  abstract TSKey getIndexKey();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setRatioList(PCollectionView<Map<TSKey, Double>> newRatioList);

    public abstract Builder setIndexList(Set<TSKey> newIndexList);

    public abstract Builder setDownSampleWindowLength(Duration newDownSampleWindowLength);

    public abstract Builder setAbsoluteStopTime(Instant newAbsoluteStopTime);

    public abstract Builder setTimeToLive(Duration newTimeToLive);

    public abstract Builder setIndexKey(TSKey newIndexKey);

    public abstract BlendedIndex build();
  }

  public static Builder builder() {
    return new AutoValue_BlendedIndex.Builder();
  }

  @Override
  public PCollection<TSDataPoint> expand(PCollection<TSDataPoint> input) {

    MergeSparseStreamsToSingleDenseStream merge =
        MergeSparseStreamsToSingleDenseStream.builder()
            .setAbsoluteStopTime(getAbsoluteStopTime())
            .setDownSampleWindowLength(getDownSampleWindowLength())
            .setIndexList(getIndexList())
            .setTimeToLive(getTimeToLive())
            .setNewIndexKey(getIndexKey())
            .build();

    return input.apply(merge).apply(ParDo.of(Blend.create(this)).withSideInputs(getRatioList()));
  }

  @AutoValue
  abstract static class Blend extends DoFn<Iterable<TSDataPoint>, TSDataPoint> {

    abstract BlendedIndex getBlendedIndex();

    public static Blend create(BlendedIndex newBlendedIndex) {
      return builder().setBlendedIndex(newBlendedIndex).build();
    }

    public static Builder builder() {
      return new AutoValue_BlendedIndex_Blend.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setBlendedIndex(BlendedIndex newBlendedIndex);

      public abstract Blend build();
    }

    @ProcessElement
    public void process(
        ProcessContext pc,
        @Element Iterable<TSDataPoint> element,
        @Timestamp Instant timestamp,
        OutputReceiver<TSDataPoint> o) {

      BigDecimal sum = new BigDecimal(0);

      List<TSDataPoint> datapoints = new ArrayList<>();

      Set<TSKey> seenKeys = new HashSet<>();

      for (TSDataPoint value : element) {
        datapoints.add(value);
        BigDecimal item_value = computeRatio(value, pc.sideInput(getBlendedIndex().getRatioList()));
        sum = sum.add(item_value);
        seenKeys.add(value.getKey());
      }

      // We must have all values, else we will refuse to output
      if (getBlendedIndex().getIndexList().equals(seenKeys)) {
        o.output(
            TSDataPoint.newBuilder()
                .setKey(getBlendedIndex().getIndexKey())
                .setTimestamp(Timestamps.fromMillis(timestamp.getMillis()))
                .setData(Data.newBuilder().setDoubleVal(sum.doubleValue()))
                .putAllMetadata(CommonUtils.mergeDataPoints(datapoints))
                .build());
      } else {

        LOG.error(
            "Blended Index did not have all values for the expected indexes {}, only had {}, will not output value.",
            getBlendedIndex().getIndexList(),
            seenKeys);
      }
    }

    private BigDecimal computeRatio(TSDataPoint dataPoint, Map<TSKey, Double> ratios) {

      Double ratio = ratios.get(dataPoint.getKey());

      Preconditions.checkNotNull(
          ratio, String.format("There is no ratio provided for key %s", dataPoint.getKey()));

      BigDecimal value = TSDataUtils.getBigDecimalFromData(dataPoint.getData());

      return value.multiply(BigDecimal.valueOf(ratio));
    }
  }
}
