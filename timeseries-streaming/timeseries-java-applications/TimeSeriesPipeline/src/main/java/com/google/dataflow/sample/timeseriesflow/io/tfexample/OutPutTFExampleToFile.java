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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesTFExampleKeys.ExampleMetadata;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.TSToTFExampleUtils.ExampleToKeyValue;
import com.google.dataflow.sample.timeseriesflow.options.TFXOptions;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
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
public abstract class OutPutTFExampleToFile extends PTransform<PCollection<Example>, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(OutPutTFExampleToFile.class);

  public abstract boolean getEnableSingleWindowFile();

  public abstract int getNumShards();

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setEnableSingleWindowFile(boolean enableSingleWindowFile);

    public abstract Builder setNumShards(int numShards);

    public abstract OutPutTFExampleToFile build();
  }

  public static OutPutTFExampleToFile create() {
    return new AutoValue_OutPutTFExampleToFile.Builder()
        .setEnableSingleWindowFile(true)
        .setNumShards(5)
        .build();
  }

  public OutPutTFExampleToFile withEnabledSingeWindowFile(boolean value) {
    return this.toBuilder().setEnableSingleWindowFile(value).build();
  }

  public OutPutTFExampleToFile setNumShards(int value) {
    return this.toBuilder().setNumShards(value).build();
  }

  public static Builder builder() {
    return new AutoValue_OutPutTFExampleToFile.Builder();
  }

  @Override
  public PDone expand(PCollection<Example> input) {

    TFXOptions options = input.getPipeline().getOptions().as(TFXOptions.class);

    PCollection<KV<String, Example>> keyedExamples = input.apply(ParDo.of(new ExampleToKeyValue()));

    if (getEnableSingleWindowFile()) {
      keyedExamples.apply(
          FileIO.<String, KV<String, Example>>writeDynamic()
              .to(String.format("%s/data", options.getInterchangeLocation()))
              .by(
                  x ->
                      String.format(
                          "%s-%s-%s",
                          x.getKey().replace("/", "-"),
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
                  Contextful.<KV<String, Example>, byte[]>fn((x -> x.getValue().toByteArray())),
                  TFRecordIO.<byte[]>sink()));
    } else {
      keyedExamples
          .apply(Window.into(FixedWindows.of(Duration.standardHours(1))))
          .apply(
              FileIO.<KV<String, Example>>write()
                  .to(String.format("%s/data", options.getInterchangeLocation()))
                  .withNumShards(getNumShards())
                  .withPrefix("TS_TFExamples")
                  .withSuffix(".tfrecord")
                  .via(
                      Contextful.<KV<String, Example>, byte[]>fn((x -> x.getValue().toByteArray())),
                      TFRecordIO.<byte[]>sink()));
    }

    return PDone.in(input.getPipeline());
  }
}
