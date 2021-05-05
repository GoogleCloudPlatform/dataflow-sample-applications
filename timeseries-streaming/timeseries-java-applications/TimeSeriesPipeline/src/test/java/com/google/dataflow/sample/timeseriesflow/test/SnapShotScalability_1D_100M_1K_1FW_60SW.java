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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class SnapShotScalability_1D_100M_1K_1FW_60SW {

  public static void main(String args[]) {
    System.out.println("Running 1 Day with 1 Key and 100 features @ Type 1 1 sec Type 2 60 sec");

    ScaleTestingOptions options =
        PipelineOptionsFactory.fromArgs(args).as(ScaleTestingOptions.class);

    options.setAppName("SimpleDataStreamTSDataPoints");
    options.setTypeOneComputationsLengthInSecs(1);
    options.setTypeTwoComputationsLengthInSecs(60);
    options.setOutputTimestepLengthInSecs(60);
    options.setNumKeys(1);
    options.setNumSecs(86400);
    options.setNumFeatures(1);

    Pipeline p = Pipeline.create(options);

    SnapShotUtils.testSnapShotScalability(p);

    p.run();
  }
}
