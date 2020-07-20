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
package com.google.dataflow.sample.retail.businesslogic.core.utils;

import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** Wrapper to ensure that timestamp attribute has been set for PubSub. */
@Experimental
public class ReadPubSubMsgPayLoadAsString extends PTransform<PBegin, PCollection<String>> {

  private String pubsubTopic;

  public ReadPubSubMsgPayLoadAsString(String pubsubTopic) {
    this.pubsubTopic = pubsubTopic;
  }

  public ReadPubSubMsgPayLoadAsString(@Nullable String name, String pubsubTopic) {
    super(name);
    this.pubsubTopic = pubsubTopic;
  }

  @Override
  public PCollection<String> expand(PBegin input) {
    /**
     * **********************************************************************************************
     * Read our events from PubSub topics. The values are sent as JSON.
     * **********************************************************************************************
     */
    PCollection<String> pubSubMessages =
        input.apply(
            "ReadStream",
            PubsubIO.readStrings()
                .fromSubscription(pubsubTopic)
                .withTimestampAttribute("TIMESTAMP"));

    /** Output raw values if in debug mode. */
    if (input.getPipeline().getOptions().as(RetailPipelineOptions.class).getDebugMode()) {
      pubSubMessages.apply(
          ParDo.of(
              new DoFn<String, Void>() {
                @ProcessElement
                public void process(ProcessContext pc) {
                  System.out.println(pc.element());
                }
              }));
    }
    return pubSubMessages;
  }
}
