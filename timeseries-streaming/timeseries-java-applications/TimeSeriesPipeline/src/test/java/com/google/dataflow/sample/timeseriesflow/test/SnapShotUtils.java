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
package com.google.dataflow.sample.timeseriesflow.test;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.MajorKeyWindowSnapshot;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SnapShotUtils {

  public static PCollection<Iterable<TSAccumSequence>> testSnapShotScalability(Pipeline p) {

    ScaleTestingOptions options = p.getOptions().as(ScaleTestingOptions.class);

    TSKey key = TSDataTestUtils.KEY_A_A;

    // Create Metrics
    TSAccum.Builder tsAccumBuilder = TSAccum.newBuilder().setKey(key);

    for (double i = 0D; i < (double) options.getNumFeatures(); i++) {
      tsAccumBuilder.putDataStore(String.valueOf(i), Data.newBuilder().setDoubleVal(i).build());
    }

    return p.apply(Create.of(tsAccumBuilder.build()))
        .apply(ParDo.of(new SnapShotUtils.GenerateKeys(options.getNumKeys())))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new SnapShotUtils.GenerateData(options.getNumSecs())))
        .apply(
            ConvertAccumToSequence.builder()
                .setWindow(
                    Window.into(
                        SlidingWindows.of(
                                Duration.standardSeconds(
                                    options.getTypeTwoComputationsLengthInSecs()))
                            .every(
                                Duration.standardSeconds(
                                    options.getTypeOneComputationsLengthInSecs()))))
                .build())
        .apply(MajorKeyWindowSnapshot.generateWindowSnapshot());
  }

  private static class GenerateKeys extends DoFn<TSAccum, TSAccum> {

    final int keyCount;

    public GenerateKeys(int keyCount) {
      this.keyCount = keyCount;
    }

    @ProcessElement
    public void process(@Element TSAccum input, OutputReceiver<TSAccum> o) {

      for (int i = 0; i < keyCount; i++) {
        TSKey key =
            input.getKey().toBuilder().setMajorKey(input.getKey().getMajorKey() + i).build();

        o.output(input.toBuilder().setKey(key).build());
        ;
      }
    }
  }

  private static class GenerateData extends DoFn<TSAccum, KV<TSKey, TSAccum>> {

    final int accumCount;

    Instant start = Instant.ofEpochMilli(TSDataTestUtils.START);

    public GenerateData(int accumCount) {
      this.accumCount = accumCount;
    }

    @ProcessElement
    public void process(@Element TSAccum input, OutputReceiver<KV<TSKey, TSAccum>> o) {

      for (int i = 0; i < accumCount; i++) {
        o.outputWithTimestamp(
            KV.of(
                input.getKey(),
                input
                    .toBuilder()
                    .setLowerWindowBoundary(
                        Timestamps.fromMillis(start.plus(Duration.standardSeconds(i)).getMillis()))
                    .setUpperWindowBoundary(
                        Timestamps.fromMillis(
                            start.plus(Duration.standardSeconds(i + 1)).getMillis()))
                    .build()),
            start.plus(Duration.standardSeconds(i)));
      }
    }
  }

  public static class IterableTSAccumTSKeyDoFn extends DoFn<Iterable<TSAccum>, TSKey> {
    @ProcessElement
    public void process(ProcessContext pc) {
      pc.element().forEach(x -> pc.output(x.getKey()));
    }
  }

  public static class IterableTSAccumSequenceTSKeyDoFn
      extends DoFn<Iterable<TSAccumSequence>, TSKey> {
    @ProcessElement
    public void process(ProcessContext pc) {
      pc.element().forEach(x -> pc.output(x.getKey()));
    }
  }
}
