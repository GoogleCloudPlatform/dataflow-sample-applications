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
package com.google.dataflow.sample.timeseriesflow.metrics.core.complex.rule;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is intended as an example tool to demonstrate how rules could be applied to the output
 * of the metrics library. The example blends several metric output streams together and applied a
 * series of sample rules to that output.
 *
 * <p>The example makes use of several metric streams:
 *
 * <p>1- VWAP metric 2- The LAST known Price. 3- The LAST known Ask. 4- The LAST known Bid.
 *
 * <p>The rule is as follows. *
 *
 * <p>1 - If there is a valid non zero VWAP within the {@link
 * GenerateComputations#getType2SlidingWindowDuration()} then the output == VWAP
 *
 * <p>2 - If there is no non-zero VWAP values within the {@link
 * GenerateComputations#getType2SlidingWindowDuration()} then the following rules are applied:
 *
 * <p>2a - If the last known price value is > a lower bound (Bid) and < a higher bound (Ask) then
 * the primary is used. The lower and upper bound are based on either Type 1 or Type 2 computations.
 *
 * <p>2b - If the last known primary value is < a lower bound (Bid) then the lower bound is used.
 * The lower bound is based on either Type 1 or Type 2 computations.
 *
 * <p>2c - If the last known primary value is > a higher bound (Ask) then the higher bound is used.
 * The lower bound is based on either Type 1 or Type 2 computations.
 *
 * <p>Edge conditions: If no VWAP or Price value then no metric is output. If either lower or upper
 * value is missing, then the primary value is output. A error log will also be raised.
 *
 * <p>Limitations:
 *
 * <p>This transform currently requires that the needed primary / lower and upper metrics have been
 * already been added to {@link GenerateComputations} before this metric is added to the list of
 * complex metrics. For example if the primary value requires the {@link VWAPGFn}, then the call to
 * {@link GenerateComputations#getComplexType2Metrics()} must have {@link VWAPGFn} before this class
 * in the list.
 */
@Experimental
public class ValueInBoundsGFn
    extends PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ValueInBoundsGFn.class);

  /** Options for {@link ValueInBoundsGFn} fn. */
  public interface ValueInBoundsOptions extends TSFlowOptions {

    List<String> getValueInBoundsMajorKeyName();

    void setValueInBoundsMajorKeyName(List<String> majorKeyName);

    String getValueInBoundsOutputMetricName();

    void setValueInBoundsOutputMetricName(String majorKeyName);

    String getValueInBoundsPrimaryMinorKeyName();

    void setValueInBoundsPrimaryMinorKeyName(String primaryMinorKeyName);

    String getValueInBoundsPrimaryMetricName();

    void setValueInBoundsPrimaryMetricName(String primaryMetricName);

    String getValueInBoundsLowerBoundaryMinorKeyName();

    void setValueInBoundsLowerBoundaryMinorKeyName(String lowerBoundaryMinorKeyName);

    String getValueInBoundsUpperBoundaryMinorKeyName();

    void setValueInBoundsUpperBoundaryMinorKeyName(String lowerBoundaryMinorKeyName);
  }

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccum>> input) {

    ValueInBoundsOptions vwapOptions =
        input.getPipeline().getOptions().as(ValueInBoundsOptions.class);

    List<TSKey> keys = new ArrayList<>();

    // Generate list of all keys that the user requested for bound validation

    for (String key : vwapOptions.getValueInBoundsMajorKeyName()) {
      TSKey.Builder builder = TSKey.newBuilder().setMajorKey(key);
      keys.add(
          builder.setMinorKeyString(vwapOptions.getValueInBoundsPrimaryMinorKeyName()).build());
      keys.add(
          builder
              .setMinorKeyString(vwapOptions.getValueInBoundsLowerBoundaryMinorKeyName())
              .build());
      keys.add(
          builder
              .setMinorKeyString(vwapOptions.getValueInBoundsUpperBoundaryMinorKeyName())
              .build());
    }

    Filter<KV<TSKey, TSAccum>> filterInclude = Filter.by(x -> keys.contains(x.getKey()));
    Filter<KV<TSKey, TSAccum>> filterExclude = Filter.by(x -> !keys.contains(x.getKey()));

    return PCollectionList.of(input.apply("Filter_1", filterExclude))
        .and(
            input
                .apply("Filter_2", filterInclude)
                .apply(WithKeys.of(x -> x.getKey().toBuilder().clearMinorKeyString().build()))
                .setCoder(KvCoder.of(ProtoCoder.of(TSKey.class), CommonUtils.getKvTSAccumCoder()))
                .apply(GroupByKey.create())
                .apply(Values.create())
                .apply(
                    ParDo.of(
                        ApplyRules.builder()
                            .setPrimaryMinorKeyName(
                                vwapOptions.getValueInBoundsPrimaryMinorKeyName())
                            .setPrimaryMetricName(vwapOptions.getValueInBoundsPrimaryMetricName())
                            .setLowerBoundaryMinorKeyName(
                                vwapOptions.getValueInBoundsLowerBoundaryMinorKeyName())
                            .setUpperBoundaryMinorKeyName(
                                vwapOptions.getValueInBoundsUpperBoundaryMinorKeyName())
                            .setOutputMetricName(vwapOptions.getValueInBoundsOutputMetricName())
                            .build())))
        .apply(Flatten.pCollections());
  }

  @VisibleForTesting
  @AutoValue
  public abstract static class ApplyRules
      extends DoFn<Iterable<KV<TSKey, TSAccum>>, KV<TSKey, TSAccum>> {

    abstract String getOutputMetricName();

    abstract String getPrimaryMinorKeyName();

    abstract String getPrimaryMetricName();

    abstract String getLowerBoundaryMinorKeyName();

    abstract String getUpperBoundaryMinorKeyName();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setOutputMetricName(String newOutputMetricName);

      public abstract Builder setPrimaryMinorKeyName(String newPrimaryMinorKeyName);

      public abstract Builder setPrimaryMetricName(String newPrimaryMetricName);

      public abstract Builder setLowerBoundaryMinorKeyName(String newLowerBoundaryMinorKeyName);

      public abstract Builder setUpperBoundaryMinorKeyName(String newUpperBoundaryMinorKeyName);

      public abstract ApplyRules build();
    }

    public static Builder builder() {
      return new AutoValue_ValueInBoundsGFn_ApplyRules.Builder();
    }

    @ProcessElement
    public void process(
        @Element Iterable<KV<TSKey, TSAccum>> element, OutputReceiver<KV<TSKey, TSAccum>> o) {

      // Create outputKey, which is the MajorKey and MinorKey from Primary and a new Metric.
      AccumWithinBoundary output = null;

      KV<TSKey, TSAccum> primaryAccum = null;
      Data primary = null;
      Data primaryLast = null;

      Data lower = null;
      Data upper = null;

      for (KV<TSKey, TSAccum> kvAccum : element) {
        if (kvAccum.getKey().getMinorKeyString().equals(getPrimaryMinorKeyName())) {
          primaryAccum = kvAccum;
          output = new AccumWithinBoundary(kvAccum.getValue(), getOutputMetricName());
          primary = kvAccum.getValue().getDataStoreOrDefault(getPrimaryMetricName(), null);
          primaryLast = kvAccum.getValue().getDataStoreOrDefault(Indicators.LAST.name(), null);
        }
        if (kvAccum.getKey().getMinorKeyString().equals(getLowerBoundaryMinorKeyName())) {
          lower = kvAccum.getValue().getDataStoreOrDefault(Indicators.LAST.name(), null);
          // We will not be mutating this value so output as is now we have the data
          o.output(kvAccum);
        }
        if (kvAccum.getKey().getMinorKeyString().equals(getUpperBoundaryMinorKeyName())) {
          upper = kvAccum.getValue().getDataStoreOrDefault(Indicators.LAST.name(), null);
          // We will not be mutating this value so output as is now we have the data
          o.output(kvAccum);
        }
      }

      // If we have no primary then we do not output any metrics. And there is no primary to pass
      // on.
      if (primary == null) {
        LOG.error(
            "No Primary value available, normally caused by lack of bootstrap values or  misconfiguration of primary keys {}. Metric {} will not be output",
            ImmutableList.of(getPrimaryMinorKeyName(), getPrimaryMetricName()),
            getOutputMetricName());
        return;
      }

      // If the primary has non gap-fill inputs then return primary
      if (!output.build().getIsAllGapFillMessages()) {
        output.setMetric(primary);
        o.output(KV.of(output.build().getKey(), output.build()));
        return;
      }

      // If we do not have a LOWER or UPPER value then we auto output Primary
      if (lower == null || upper == null) {
        output.setMetric(primary);
        o.output(KV.of(output.build().getKey(), output.build()));
        return;
      }

      // We will check against the LAST value from the minor key rather than the primary

      output.setMetric(checkInSpread(primaryLast, lower, upper));
      o.output(KV.of(output.build().getKey(), output.build()));
    }
  }

  @VisibleForTesting
  public static Data checkInSpread(Data primary, Data lower, Data upper) {

    if (TSDataUtils.findMinData(primary, lower).equals(primary)) {
      return lower;
    }

    if (TSDataUtils.findMaxValue(primary, upper).equals(primary)) {
      return upper;
    }

    return primary;
  }

  public static class AccumWithinBoundary extends AccumCoreNumericBuilder {

    private final String metricName;

    public AccumWithinBoundary(TSAccum tsAccum, String within_boundary_name) {
      super(tsAccum);
      Preconditions.checkNotNull(within_boundary_name);
      metricName = within_boundary_name;
    }

    public void setMetric(Data data) {
      setValue(metricName, data);
    }

    public Data getMetricOrNull() {
      return getValueOrNull(metricName);
    }
  }
}
