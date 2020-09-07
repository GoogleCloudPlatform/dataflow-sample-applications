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
package com.google.dataflow.sample.timeseriesflow.io.tfexample;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TFXOptions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesTFExampleKeys.ExampleMetadata;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;

/**
 * Converts {@link Iterable<TSAccumSequence>>} to TF.Examples and outputs them to file location or
 * Google Cloud PubSub. NOTE: The output is placed into TF.Example rather than TF.SequenceExample
 * files as a temporary measure. This will be changed once the TFX work for support of
 * SequenceExample is in place.
 */
@Experimental
@AutoValue
public abstract class OutPutTFExampleFromTSSequence
    extends PTransform<PCollection<Iterable<TSAccumSequence>>, PCollection<Example>> {

  private static final Logger LOG = LoggerFactory.getLogger(OutPutTFExampleFromTSSequence.class);

  public abstract boolean getEnableSingleWindowFile();

  public abstract int getNumShards();

  public abstract @Nullable String getPubSubTopic();

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setEnableSingleWindowFile(boolean enableSingleWindowFile);

    public abstract Builder setPubSubTopic(String pubSubTopic);

    public abstract Builder setNumShards(int numShards);

    public abstract OutPutTFExampleFromTSSequence build();
  }

  public static OutPutTFExampleFromTSSequence create() {
    return new AutoValue_OutPutTFExampleFromTSSequence.Builder()
        .setEnableSingleWindowFile(true)
        .setNumShards(5)
        .build();
  }

  public OutPutTFExampleFromTSSequence withEnabledSingeWindowFile(boolean value) {
    return this.toBuilder().setEnableSingleWindowFile(value).build();
  }

  public OutPutTFExampleFromTSSequence setNumShards(int value) {
    return this.toBuilder().setNumShards(value).build();
  }

  public OutPutTFExampleFromTSSequence withPubSubTopic(String value) {
    return this.toBuilder().setPubSubTopic(value).build();
  }

  public static Builder builder() {
    return new AutoValue_OutPutTFExampleFromTSSequence.Builder();
  }

  @Override
  public PCollection<Example> expand(PCollection<Iterable<TSAccumSequence>> input) {

    TFXOptions options = input.getPipeline().getOptions().as(TFXOptions.class);
    Preconditions.checkNotNull(
        options.getInterchangeLocation(),
        "If output to file is enabled, --interchangeLocation option must be set.");

    /** Convert each major key to a TF.Example */
    PCollection<KV<TSKey, Example>> results =
        input.apply(
            new TSAccumIterableToTFExample(
                String.format("%s/metadata", options.getInterchangeLocation())));

    PCollection<Example> values = results.apply(Values.create());
    if (getPubSubTopic() != null) {
      values
          .apply(
              ParDo.of(
                  // TODO add to utility classes
                  new DoFn<Example, String>() {
                    @ProcessElement
                    public void processElement(@Element Example example, OutputReceiver<String> o) {
                      try {
                        String jsonFormat = JsonFormat.printer().print(example);
                        o.output(jsonFormat);
                      } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                      }
                    }
                  }))
          .apply(PubsubIO.writeStrings().to(getPubSubTopic()));
    }

    if (getEnableSingleWindowFile()) {
      results.apply(
          FileIO.<String, KV<TSKey, Example>>writeDynamic()
              .to(String.format("%s/data", options.getInterchangeLocation()))
              .by(
                  x ->
                      String.format(
                          "%s-%s-%s",
                          x.getKey().getMajorKey().replace("/", "-"),
                          x.getValue()
                              .getFeatures()
                              .getFeatureMap()
                              .get(ExampleMetadata.METADATA_SPAN_START_TS.name())
                              .getInt64List()
                              .getValue(0),
                          x.getValue()
                              .getFeatures()
                              .getFeatureMap()
                              .get(ExampleMetadata.METADATA_SPAN_END_TS.name())
                              .getInt64List()
                              .getValue(0)))
              .withDestinationCoder(StringUtf8Coder.of())
              .withNumShards(getNumShards())
              .withNaming(x -> FileIO.Write.defaultNaming(x, ".tfrecord"))
              .via(
                  Contextful.<KV<TSKey, Example>, byte[]>fn((x -> x.getValue().toByteArray())),
                  TFRecordIO.<byte[]>sink()));
    } else {
      results
          .apply(Window.into(FixedWindows.of(Duration.standardHours(1))))
          .apply(
              FileIO.<KV<TSKey, Example>>write()
                  .to(String.format("%s/data", options.getInterchangeLocation()))
                  .withNumShards(getNumShards())
                  .withPrefix("Timeseries_TFExamples_")
                  .withSuffix(".tfrecord")
                  .via(
                      Contextful.<KV<TSKey, Example>, byte[]>fn((x -> x.getValue().toByteArray())),
                      TFRecordIO.<byte[]>sink()));
    }

    return values;
  }
}
