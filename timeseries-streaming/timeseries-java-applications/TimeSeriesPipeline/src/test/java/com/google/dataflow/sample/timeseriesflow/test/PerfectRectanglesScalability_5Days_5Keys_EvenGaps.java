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

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PerfectRectanglesScalability_5Days_5Keys_EvenGaps {

  public static void main(String args[]) {
    System.out.println("Running 1 Day 86400 with 1 Key");

    ScaleTestingOptions options =
        PipelineOptionsFactory.fromArgs(args).as(ScaleTestingOptions.class);

    options.setAppName("TestPerfectRectangles_86400S_NoGaps");
    options.setTypeOneComputationsLengthInSecs(1);
    options.setTypeTwoComputationsLengthInSecs(60);
    options.setOutputTimestepLengthInSecs(60);
    options.setTTLDurationSecs(2);
    options.setNumKeys(5);
    options.setPerfectRecNumberDataSecs(86400 * 5);
    options.setRunner(DataflowRunner.class);
    options.setSkipEvens(true);

    Pipeline p = Pipeline.create(options);

    PerfectRectangleUtils.testPerfectRecScalability(p);

    long time = System.currentTimeMillis();
    p.run();
    System.out.println(System.currentTimeMillis() - time);
  }
}
