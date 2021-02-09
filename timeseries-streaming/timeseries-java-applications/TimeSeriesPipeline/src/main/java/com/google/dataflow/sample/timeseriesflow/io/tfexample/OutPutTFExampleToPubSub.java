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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
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
public abstract class OutPutTFExampleToPubSub extends PTransform<PCollection<Example>, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(OutPutTFExampleToPubSub.class);

  public abstract @Nullable String getPubSubTopic();

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setPubSubTopic(String pubSubTopic);

    public abstract OutPutTFExampleToPubSub build();
  }

  public OutPutTFExampleToPubSub withPubSubTopic(String value) {
    return this.toBuilder().setPubSubTopic(value).build();
  }

  public static Builder builder() {
    return new AutoValue_OutPutTFExampleToPubSub.Builder();
  }

  public static OutPutTFExampleToPubSub create(String pubSubTopic) {
    return new AutoValue_OutPutTFExampleToPubSub.Builder().setPubSubTopic(pubSubTopic).build();
  }

  @Override
  public PDone expand(PCollection<Example> input) {

    return input
        .apply(
            ParDo.of(
                // TODO add to utility classes
                new DoFn<Example, String>() {
                  @ProcessElement
                  public void processElement(@Element Example example, OutputReceiver<String> o) {
                    try {
                      String jsonFormat =
                          JsonFormat.printer().omittingInsignificantWhitespace().print(example);
                      o.output(jsonFormat);
                    } catch (InvalidProtocolBufferException e) {
                      LOG.error(e.getMessage());
                    }
                  }
                }))
        .apply(PubsubIO.writeStrings().to(getPubSubTopic()));
  }
}
