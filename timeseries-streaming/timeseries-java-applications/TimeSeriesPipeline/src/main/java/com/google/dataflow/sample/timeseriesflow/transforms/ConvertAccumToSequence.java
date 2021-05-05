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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence.Builder;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
/**
 * Creates a {@link TSAccumSequence} from a {@link TSAccum} given a {@link Window}. Without init
 * bootstrap there will be {@link TSAccumSequences} which will have imbalanced length use {@link
 * ConvertAccumToSequenceV2.Builder#setCheckHasValueCountOf(Integer)}
 *
 * <p>If no {@link ConvertAccumToSequence#getWindow()} is set then {@link
 * TSFlowOptions#getOutputTimestepLengthInSecs()} will be used. The default will be a Sliding window
 * of duration {@link TSFlowOptions#getOutputTimestepLengthInSecs()} and offset {@link
 * TSFlowOptions#getTypeOneComputationsLengthInSecs()}
 */
@Experimental
public abstract class ConvertAccumToSequence
    extends PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccumSequence>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ConvertAccumToSequence.class);

  @Nullable
  public abstract Window<KV<TSKey, TSAccum>> getWindow();

  public abstract Builder toBuilder();

  public @Nullable abstract Integer getCheckHasValueCountOf();

  public static Builder builder() {
    return new AutoValue_ConvertAccumToSequence.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setWindow(Window<KV<TSKey, TSAccum>> window);

    public abstract Builder setCheckHasValueCountOf(Integer count);

    public abstract ConvertAccumToSequence build();
  }

  @Override
  public PCollection<KV<TSKey, TSAccumSequence>> expand(PCollection<KV<TSKey, TSAccum>> input) {

    PCollection<KV<TSKey, TSAccum>> windowedInput;

    if (this.getWindow() != null) {
      windowedInput = input.apply(this.getWindow());
    } else {
      LOG.info(
          "No window passed, will use Sliding window from options output timestep and type 1 fixed length.");
      TSFlowOptions options = input.getPipeline().getOptions().as(TSFlowOptions.class);
      windowedInput =
          input.apply(
              Window.into(
                  SlidingWindows.of(
                          Duration.standardSeconds(options.getOutputTimestepLengthInSecs()))
                      .every(
                          Duration.standardSeconds(options.getTypeOneComputationsLengthInSecs()))));
    }

    return windowedInput
        .apply(Combine.perKey(new AccumToSequenceCombiner()))
        .setCoder(CommonUtils.getKvTSAccumSequenceCoder())
        .apply("TSAccumToSequence", ParDo.of(new AccumToSequencesDoFn(this)));
  }

  public static class AccumToSequencesDoFn
      extends DoFn<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccumSequence>> {

    ConvertAccumToSequence convertAccumToSequence;

    public AccumToSequencesDoFn(ConvertAccumToSequence convertAccumToSequence) {
      this.convertAccumToSequence = convertAccumToSequence;
    }

    @ProcessElement
    public void process(ProcessContext pc, IntervalWindow w) {

      // Memory pressure based on element size * N
      // This is mitigated as only aggregator's are loaded.
      // TODO Convert to use the sorter utility for larger sorts

      // Make new list to allow sort
      List<TSAccum> inMemoryList = new ArrayList<>(pc.element().getValue().getAccumsList());

      if (convertAccumToSequence.getCheckHasValueCountOf() != null
          && inMemoryList.size() != convertAccumToSequence.getCheckHasValueCountOf()) {
        LOG.info(
            String.format(
                "Discarding TSAccumSequence with window %s as it does not have enough samples yet. Expected %s Count %s",
                w, convertAccumToSequence.getCheckHasValueCountOf(), inMemoryList.size()));
        return;
      }

      // TODO this verification check needs to be moved to a more formal verifier
      // TODO also important to note that this is bugged as it does not have metrics across whole
      // run
      Iterator<TSAccum> accum = inMemoryList.iterator();
      Set<String> listOfMetrics = new HashSet<>(accum.next().getDataStoreMap().keySet());
      while (accum.hasNext()) {
        if (!listOfMetrics.containsAll(accum.next().getDataStoreMap().keySet())) {
          LOG.warn("Discarding TSAccumSequence as not all metrics are present in every sample");
          return;
        }
      }

      TSAccumSequence.Builder sequence = TSAccumSequence.newBuilder();

      sequence.setKey(pc.element().getKey());
      sequence.setLowerWindowBoundary(Timestamps.fromMillis(w.start().getMillis()));
      sequence.setUpperWindowBoundary(Timestamps.fromMillis(w.end().getMillis()));

      inMemoryList.sort(Comparator.comparing(x -> Timestamps.toMillis(x.getLowerWindowBoundary())));

      inMemoryList.forEach(sequence::addAccums);

      sequence.setDuration(Durations.fromMillis(w.end().getMillis() - w.start().getMillis()));

      sequence.setCount(inMemoryList.size());

      pc.output(KV.of(pc.element().getKey(), sequence.build()));
    }
  }

  public static class AccumToSequenceCombiner
      extends CombineFn<TSAccum, TSAccumSequence, TSAccumSequence> {

    @Override
    public TSAccumSequence createAccumulator() {
      return TSAccumSequence.newBuilder().build();
    }

    @Override
    public TSAccumSequence addInput(TSAccumSequence accumulator, TSAccum input) {
      return accumulator.toBuilder().addAccums(input).build();
    }

    @Override
    public TSAccumSequence mergeAccumulators(Iterable<TSAccumSequence> accumulators) {
      if (!accumulators.iterator().hasNext()) {
        return TSAccumSequence.newBuilder().build();
      }

      Iterator<TSAccumSequence> iterator = accumulators.iterator();
      TSAccumSequence.Builder accumaltor = iterator.next().toBuilder();
      while (iterator.hasNext()) {
        accumaltor.addAllAccums(iterator.next().getAccumsList());
      }
      return accumaltor.build();
    }

    @Override
    public TSAccumSequence extractOutput(TSAccumSequence accumulator) {
      return accumulator;
    }
  }
}
