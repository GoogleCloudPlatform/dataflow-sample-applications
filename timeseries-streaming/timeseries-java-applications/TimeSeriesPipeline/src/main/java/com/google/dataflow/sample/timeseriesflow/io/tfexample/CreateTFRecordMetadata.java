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
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.tensorflow.example.Example;

@Experimental
/**
 * Output Metadata about the TF Example files being output by the pipeline. This is not yet used by
 * the sample.
 */
public class CreateTFRecordMetadata extends PTransform<PCollection<Example>, WriteFilesResult> {

  public final String FILE_PREFIX = "TimeSeriesMetaData";
  public final String FEATURE_KEY = "NAME";

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
    // TODO This output is too verbose, output per window is not needed, switch to only outputting
    // mutations.
    return input
        .apply(
            WithKeys.of(
                x ->
                    x.getFeatures()
                        .getFeatureMap()
                        .get(FEATURE_KEY)
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
                .withPrefix(FILE_PREFIX)
                .withNumShards(1)
                .via(
                    Contextful.<Example, byte[]>fn((x -> x.toByteArray())),
                    TFRecordIO.<byte[]>sink()));
  }
}
