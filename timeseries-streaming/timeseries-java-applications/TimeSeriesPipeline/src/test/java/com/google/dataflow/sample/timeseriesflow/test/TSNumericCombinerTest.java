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

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSNumericCombiner;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TSNumericCombinerTest {

  @Test
  public void testNumericCombine() {

    Instant instant = Instant.parse("2000-01-01T00:00:00");
    Timestamp timeA = Timestamps.fromMillis(instant.getMillis());
    Timestamp timeB = Timestamps.fromMillis(instant.plus(Duration.standardSeconds(10)).getMillis());

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    TSKey key = TSKey.newBuilder().setMajorKey("Major").setMajorKey("Minor").build();
    TSDataPoint a =
        TSDataPoint.newBuilder()
            .setKey(key)
            .setTimestamp(timeA)
            .setData(Data.newBuilder().setIntVal(1))
            .build();
    TSDataPoint b =
        TSDataPoint.newBuilder(a)
            .setTimestamp(timeB)
            .setData(Data.newBuilder().setIntVal(2))
            .build();
    PCollection<TSAccum> collection =
        p.apply(Create.of(a, b))
            .apply(WithKeys.of(TSDataPoint::getKey))
            .setCoder(CommonUtils.getKvTSDataPointCoder())
            .apply(Combine.perKey(TSNumericCombiner.combine()))
            .apply(Values.create());

    TSAccum.Builder output = TSAccum.newBuilder().setKey(key);

    output.putDataStore(
        Indicators.FIRST_TIMESTAMP.name(), CommonUtils.createNumData(Timestamps.toMillis(timeA)));
    output.putDataStore(
        Indicators.LAST_TIMESTAMP.name(), CommonUtils.createNumData(Timestamps.toMillis(timeB)));
    output.putDataStore(Indicators.FIRST.name(), CommonUtils.createNumData(1));
    output.putDataStore(Indicators.LAST.name(), CommonUtils.createNumData(2));
    output.putDataStore(Indicators.SUM.name(), CommonUtils.createNumData(3));
    output.putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(2));
    output.putDataStore(Indicators.MIN.name(), CommonUtils.createNumData(1));
    output.putDataStore(Indicators.DATA_POINT_COUNT.name(), CommonUtils.createNumData(2L));

    PAssert.that(collection).containsInAnyOrder(output.build());
    p.run();
  }
}
