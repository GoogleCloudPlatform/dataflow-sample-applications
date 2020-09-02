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
package com.google.dataflow.sample.timeseriesflow.examples.simpledata.xlang;

import com.google.dataflow.sample.timeseriesflow.examples.simpledata.transforms.Print;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.runners.core.construction.External;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;

public class XLangRunInference {

  private static final String RUN_INFERENCE_URN = "beam:transforms:xlang:tfx:run_inference";

  public static void main(String[] args) throws Exception {

    String[] input = new String[1];
    input[0] = "--experiments=beam_fn_api";
    PortablePipelineOptions portablePipelineOptions =
        PipelineOptionsFactory.fromArgs(input).create().as(PortablePipelineOptions.class);
    portablePipelineOptions.setJobEndpoint("127.0.0.1:8099");
    portablePipelineOptions.setRunner(PortableRunner.class);

    Pipeline p = Pipeline.create(portablePipelineOptions);

    String fileLocation = "gs://<>/sample_data/inference/data/*";
    String savedModelFile = "gs://<>/sample_data/saved_model_file/";

    p.apply(TFRecordIO.read().from(fileLocation))
        .apply(
            External.of(RUN_INFERENCE_URN, toStringPayloadBytes(savedModelFile), "localhost:8899"))
        .apply(new Print<>());

    State state = p.run().waitUntilFinish();
    System.out.println(state);
  }

  public static byte[] toStringPayloadBytes(String data) throws IOException {
    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .putConfiguration(
                "data",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn("beam:coder:string_utf8:v1")
                    .setPayload(ByteString.copyFrom(encodeString(data)))
                    .build())
            .build();
    return payload.toByteArray();
  }

  public static byte[] encodeString(String str) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringUtf8Coder.of().encode(str, baos);
    return baos.toByteArray();
  }
}
