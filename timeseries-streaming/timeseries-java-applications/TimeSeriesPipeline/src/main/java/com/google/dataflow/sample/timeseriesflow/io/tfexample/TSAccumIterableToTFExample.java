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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.options.TFXOptions;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;

/**
 * Convert a TSAccum to a TFExample. If fileBase is set there will be a Metadata file output per
 * window. This is a place holder for future functionality.
 */
@Experimental
public class TSAccumIterableToTFExample
    extends PTransform<PCollection<Iterable<TSAccumSequence>>, PCollection<Example>> {

  private static final Logger LOG = LoggerFactory.getLogger(TSAccumIterableToTFExample.class);

  String fileBase;
  Boolean enableMetadataOutput = false;
  boolean useMajorKeyInFeature = true;

  public TSAccumIterableToTFExample() {}

  public TSAccumIterableToTFExample(String fileBase) {
    this.fileBase = fileBase;
  }

  public TSAccumIterableToTFExample(String fileBase, boolean useMajorKeyInFeature) {
    this.fileBase = fileBase;
    this.useMajorKeyInFeature = useMajorKeyInFeature;
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
  public PCollection<Example> expand(PCollection<Iterable<TSAccumSequence>> input) {

    Integer timesteps =
        CommonUtils.getNumOfSequenceTimesteps(
            input.getPipeline().getOptions().as(TFXOptions.class));

    PCollectionTuple exampleAndMetadata =
        input.apply(new FeaturesFromIterableAccumSequence(timesteps, useMajorKeyInFeature));

    // Send Metadata file to output location
    if (enableMetadataOutput) {
      exampleAndMetadata
          .get(FeaturesFromIterableAccumSequence.TIME_SERIES_FEATURE_METADATA)
          .apply(new CreateTFRecordMetadata(fileBase));
    }
    // Process Example
    return exampleAndMetadata.get(FeaturesFromIterableAccumSequence.TIME_SERIES_EXAMPLES);
  }
}
