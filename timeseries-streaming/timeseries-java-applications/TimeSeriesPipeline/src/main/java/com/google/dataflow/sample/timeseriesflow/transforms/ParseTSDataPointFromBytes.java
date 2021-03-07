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
package com.google.dataflow.sample.timeseriesflow.transforms;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Return a {@link TSDataPoint} from bytes. */
@Experimental
public class ParseTSDataPointFromBytes extends DoFn<byte[], TSDataPoint> {

  private static final Logger LOG = LoggerFactory.getLogger(ParseTSDataPointFromBytes.class);

  public static ParseTSDataPointFromBytes create() {
    return new ParseTSDataPointFromBytes();
  }

  @ProcessElement
  public void process(@Element byte[] input, OutputReceiver<TSDataPoint> o) {
    try {
      TSDataPoint dataPoint = TSDataPoint.parseFrom(input);
      o.outputWithTimestamp(
          dataPoint, Instant.ofEpochMilli(Timestamps.toMillis(dataPoint.getTimestamp())));
    } catch (InvalidProtocolBufferException e) {
      LOG.error(e.getMessage());
    }
  }
}
