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
package com.google.dataflow.sample.timeseriesflow.examples.fsi.forex;

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.MajorKeyWindowSnapshot;
import com.google.dataflow.sample.timeseriesflow.transforms.TSAccumToRow;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Wrapper used for demonstration of the pipeline with the examples module, incorporates metric
 * creation as well as output to Google BigQuery and Google cloud storage.
 */
@Experimental
@AutoValue
public abstract class AllComputationsExamplePipeline
    extends PTransform<PCollection<TSDataPoint>, PCollection<Iterable<TSAccumSequence>>> {

  public abstract GenerateComputations getGenerateComputations();

  public abstract String getTimeseriesSourceName();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_AllComputationsExamplePipeline.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setGenerateComputations(GenerateComputations newGenerateComputations);

    public abstract Builder setTimeseriesSourceName(String newTimeseriesSourceName);

    public abstract AllComputationsExamplePipeline build();
  }

  @Override
  public PCollection<Iterable<TSAccumSequence>> expand(PCollection<TSDataPoint> input) {

    ExampleForexPipelineOptions options =
        input.getPipeline().getOptions().as(ExampleForexPipelineOptions.class);

    // ----------------- Stage 1 create Computations

    PCollection<KV<TSKey, TSAccum>> computations = input.apply(getGenerateComputations());

    // ----------------- Stage 2 output results to BigQuery

    DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY_MM_dd_HH_mm_ss");

    if (options.getBigQueryTableForTSAccumOutputLocation() != null) {
      computations
          .apply(Values.create())
          .apply(new TSAccumToRow())
          .apply(
              BigQueryIO.<Row>write()
                  .useBeamSchema()
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                  .to(
                      String.join(
                          "_",
                          options.getBigQueryTableForTSAccumOutputLocation(),
                          Optional.of(getTimeseriesSourceName()).orElse(""),
                          Instant.now().toString(formatter))));
    }

    // ----------------- Create Window of ordered output sliding windows.

    PCollection<KV<TSKey, TSAccumSequence>> sequences =
        computations.apply(
            ConvertAccumToSequence.builder()
                .setCheckHasValueCountOf(CommonUtils.getNumOfSequenceTimesteps(options))
                .setWindow(
                    Window.into(
                        SlidingWindows.of(getGenerateComputations().getType2SlidingWindowDuration())
                            .every(getGenerateComputations().getType1FixedWindow())))
                .build());

    // ----------------- Create a single snapshot of the all the values.

    PCollection<Iterable<TSAccumSequence>> multiVariateSpan =
        sequences.apply(MajorKeyWindowSnapshot.generateWindowSnapshot());

    return multiVariateSpan;
  }
}
