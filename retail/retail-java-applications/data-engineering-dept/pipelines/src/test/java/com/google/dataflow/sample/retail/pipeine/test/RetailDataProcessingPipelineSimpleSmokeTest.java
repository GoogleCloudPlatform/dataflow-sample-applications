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
package com.google.dataflow.sample.retail.pipeine.test;

import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.pipeline.RetailDataProcessingPipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RetailDataProcessingPipelineSimpleSmokeTest {

  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  RetailPipelineOptions options = PipelineOptionsFactory.as(RetailPipelineOptions.class);

  {
    options.setTestModeEnabled(true);
  }

  @Rule public transient TestPipeline pipeline = TestPipeline.fromOptions(options);

  @Test
  public void testPipeline() throws Exception {

    RetailDataProcessingPipeline retailDataProcessingPipeline = new RetailDataProcessingPipeline();

    PCollectionTuple inputs = pipeline.apply(new TestStreamGenerator());

    retailDataProcessingPipeline.testClickstreamEvents =
        inputs.get(TestStreamGenerator.CLICKSTREAM);

    retailDataProcessingPipeline.testTransactionEvents =
        inputs.get(TestStreamGenerator.TRANSACTION);

    retailDataProcessingPipeline.testStockEvents = inputs.get(TestStreamGenerator.STOCK);

    retailDataProcessingPipeline.startRetailPipeline(pipeline);
  }
}
