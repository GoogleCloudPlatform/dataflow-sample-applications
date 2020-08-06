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

import com.google.dataflow.sample.timeseriesflow.TFXOptions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.TSToTFExampleUtils.ColoaseMajorKeyDataPointsToSingleTFExample.ExampleToKeyValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;

/** Convert a TSAccum to a TFExample. */
@Experimental
public class TSAccumIterableToTFExample
    extends PTransform<
        PCollection<KV<TSKey, Iterable<TSAccumSequence>>>, PCollection<KV<TSKey, Example>>> {

  private static final Logger LOG = LoggerFactory.getLogger(TSAccumIterableToTFExample.class);

  String fileBase;
  Boolean enableMetadataOutput = false;

  public TSAccumIterableToTFExample(String fileBase) {
    this.fileBase = fileBase;
  }

  public TSAccumIterableToTFExample(@Nullable String name, String fileBase) {
    super(name);
    this.fileBase = fileBase;
  }

  // TODO re-enable once Metadata module is complete
  private TSAccumIterableToTFExample(String fileBase, Boolean enableMetadataOutput) {
    this.fileBase = fileBase;
    this.enableMetadataOutput = enableMetadataOutput;
  }

  private TSAccumIterableToTFExample(
      @Nullable String name, String fileBase, Boolean enableMetadataOutput) {
    super(name);
    this.fileBase = fileBase;
    this.enableMetadataOutput = enableMetadataOutput;
  }

  @Override
  public PCollection<KV<TSKey, Example>> expand(
      PCollection<KV<TSKey, Iterable<TSAccumSequence>>> input) {

    Integer timesteps =
        CommonUtils.getNumOfSequenceTimesteps(
            input.getPipeline().getOptions().as(TFXOptions.class));

    PCollectionTuple exampleAndMetadata =
        input.apply(TSToTFExampleUtils.createFeaturesFromIterableAccum(timesteps));

    // Send Metadata file to output location
    // Disabled for first sample

    if (enableMetadataOutput) {
      exampleAndMetadata
          .get(TSToTFExampleUtils.TIME_SERIES_FEATURE_METADATA)
          .apply(new CreateTFRecordMetadata(fileBase));
    }
    // Process Example
    return exampleAndMetadata
        .get(TSToTFExampleUtils.TIME_SERIES_EXAMPLES)
        .apply(ParDo.of(new ExampleToKeyValue()));
  }

  public static class CreateTFRecordMetadata
      extends PTransform<PCollection<Example>, WriteFilesResult> {

    String fileBase;

    public CreateTFRecordMetadata(String fileBase) {
      this.fileBase = fileBase;
    }

    public CreateTFRecordMetadata(@Nullable String name, String fileBase) {
      super(name);
      this.fileBase = fileBase;
    }

    @Override
    public WriteFilesResult expand(PCollection<Example> input) {

      // TODO Need to warn about Type change in the middle of a live stream
      return input
          .apply(
              WithKeys.of(
                  x ->
                      x.getFeatures()
                          .getFeatureMap()
                          .get("NAME")
                          .getBytesList()
                          .getValue(0)
                          .toByteArray()))
          .setCoder(KvCoder.of(ByteArrayCoder.of(), ProtoCoder.of(Example.class)))
          .apply(GroupByKey.create())
          .apply(
              MapElements.into(TypeDescriptor.of(Example.class))
                  .via(x -> x.getValue().iterator().next()))
          .apply(
              FileIO.<Example>write()
                  .<Example>to(fileBase)
                  .withPrefix("TimeSeriesMetaData")
                  .withNumShards(0)
                  .via(
                      Contextful.<Example, byte[]>fn((x -> x.toByteArray())),
                      TFRecordIO.<byte[]>sink()));
    }
  }
}
