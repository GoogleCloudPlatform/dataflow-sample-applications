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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class PerfectRectangleUtils {

  public static PCollection<KV<TSKey, TSDataPoint>> testPerfectRecScalability(Pipeline p) {

    ScaleTestingOptions options = p.getOptions().as(ScaleTestingOptions.class);

    Long start = TSDataTestUtils.START;

    TSKey key = TSDataTestUtils.KEY_A_A;

    // Create Metrics
    List<TSKey> dataPoints = new ArrayList<>();

    for (int i = 0; i < options.getNumKeys(); i++) {
      dataPoints.add(key.toBuilder().setMinorKeyString(String.valueOf(i)).build());
    }

    return p.apply(Create.of(dataPoints))
        .apply(Reshuffle.viaRandomKey())
        .apply(
            ParDo.of(
                new PerfectRectangleUtils.GenerateData(
                    options.getPerfectRecNumberDataSecs(), options.getSkipEvens())))
        .apply(PerfectRectangles.fromPipelineOptions(options));
  }

  private static class GenerateData extends DoFn<TSKey, KV<TSKey, TSDataPoint>> {

    final long lastValueTimestamp;
    final boolean skipEvens;

    Instant start = Instant.ofEpochMilli(TSDataTestUtils.START);

    public GenerateData(long lastValueTimestamp, boolean skipEvens) {
      this.lastValueTimestamp = lastValueTimestamp;
      this.skipEvens = skipEvens;
    }

    @ProcessElement
    public void process(@Element TSKey input, OutputReceiver<KV<TSKey, TSDataPoint>> o) {

      for (int i = 0; i < lastValueTimestamp; i++) {
        if (!skipEvens) {
          o.outputWithTimestamp(
              KV.of(
                  input,
                  TSDataPoint.newBuilder()
                      .setTimestamp(
                          Timestamps.fromMillis(
                              start.plus(Duration.standardSeconds(i)).getMillis()))
                      .setData(Data.newBuilder().setIntVal(i))
                      .build()),
              start.plus(Duration.standardSeconds(i)));
        } else if (i % 2 != 0) {
          o.outputWithTimestamp(
              KV.of(
                  input,
                  TSDataPoint.newBuilder()
                      .setTimestamp(
                          Timestamps.fromMillis(
                              start.plus(Duration.standardSeconds(i)).getMillis()))
                      .setData(Data.newBuilder().setIntVal(i))
                      .build()),
              start.plus(Duration.standardSeconds(i)));
        }
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
