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
package common;

import static common.TSTestDataBaseline.START;

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesDataTest;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesDataTest.AdvanceWatermarkExpression;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesDataTest.TSTimePointTest;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesDataTest.Time.TimePointCase;
import com.google.dataflow.sample.timeseriesflow.test.TestUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class TSTestData implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TSTestData.class);

  @Experimental
  public @Nullable abstract TestStream<TSDataPoint> inputTSData();

  public @Nullable abstract KV<TSKey, TSDataPoint> outputTSType1Data();

  public @Nullable abstract KV<TSKey, TSDataPoint> outputTSType2Data();

  public static Builder toBuilder() {
    return new AutoValue_TSTestData.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setInputTSData(TestStream<TSDataPoint> input);

    public Builder setInputTSDataFromJSON(
        JsonReader input, Duration outputTSType1Window, Duration outputTSType2Window)
        throws IOException {

      List<TimestampedValue<TSDataPoint>> messages = new ArrayList<>();
      input.beginArray();
      Long instant = START;
      TestStream.Builder<TSDataPoint> stream =
          TestStream.create(ProtoCoder.of(TSDataPoint.class))
              .advanceWatermarkTo(Instant.ofEpochMilli(instant));

      // Create a builder for the TSDataPoint message
      TSDataPoint.Builder tsDataPointBuilder = TSDataPoint.newBuilder();

      // Create a builder for the TSDataPoint message
      TSTimePointTest.Builder tsTimePointTestBuilder = TSTimePointTest.newBuilder();

      Boolean hintProvided = Boolean.FALSE;

      TestStream<TSDataPoint> testStream = null;

      try {
        while (input.hasNext()) {
          JsonElement element = JsonParser.parseReader(input);

          // Parse to TSDataPoint
          JsonFormat.parser()
              .ignoringUnknownFields()
              .merge(String.valueOf(element), tsDataPointBuilder);
          messages.add(TestUtils.timestampedValueFromTSDataPoint(tsDataPointBuilder.build()));
          tsDataPointBuilder.clear();

          // Parse to TSTimePointTest in case hints are provided
          JsonFormat.parser()
              .ignoringUnknownFields()
              .merge(String.valueOf(element), tsTimePointTestBuilder);
          tsTimePointTestBuilder.build();
          TimeSeriesDataTest.Time.TimePointCase timePointCase =
              tsTimePointTestBuilder.getTime().getTimePointCase();
          // Skip processing hints while timestamp has not been set
          if (timePointCase != TimePointCase.TIMEPOINT_NOT_SET) {
            hintProvided = Boolean.TRUE;
            // Hint detected get all previous events and start creating stream
            for (int i = 0; i < messages.size(); i++) {
              stream = stream.addElements(messages.get(i));
            }
            // Clear messages to buffer until next hint
            messages.clear();
            switch (timePointCase) {
              case ADVANCE_WATERMARK_SECONDS:
                instant =
                    Instant.ofEpochMilli(START)
                        .plus(
                            Duration.standardSeconds(
                                tsTimePointTestBuilder.getTime().getAdvanceWatermarkSeconds()))
                        .getMillis();
                stream = stream.advanceWatermarkTo(Instant.ofEpochMilli(instant));
                break;
              case ADVANCE_WATERMARK_EXPRESSION:
                AdvanceWatermarkExpression advanceWatermark_expression =
                    tsTimePointTestBuilder.getTime().getAdvanceWatermarkExpression();
                if (advanceWatermark_expression == AdvanceWatermarkExpression.INFINITY) {
                  testStream = stream.advanceWatermarkToInfinity();
                }
            }
          }
          tsTimePointTestBuilder.clear();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      // Skip this section if hint about time is provided in JSON
      if (!hintProvided) {
        // If hints are not provided, get length and calculate when to reset the watermark based on
        // type1 and type2 durations
        // e.g.: length = 9, Type1Window = 5s Type2Window = 15s -> Move watermark 5s every 3 data
        // points
        int length = messages.size();
        long intervalType1 = outputTSType1Window.getStandardSeconds();
        long intervalType2 = outputTSType2Window.getStandardSeconds();
        if (intervalType2 % intervalType1 != 0) {
          LOG.warn(
              "Intervals are not divisible, result will be truncated cutting off the floating point");
        }
        long interval = intervalType2 / intervalType1;
        if (length % interval != 0) {
          throw new InvalidPropertiesFormatException("Length must be divisible by interval");
        }
        for (int i = 0, j = 0; i < length; i++) {
          if (i % (length / interval) == 0 && i != 0) {
            j++;
            instant =
                Instant.ofEpochMilli(START)
                    .plus(Duration.standardSeconds(intervalType1 * j))
                    .getMillis();
            stream = stream.advanceWatermarkTo(Instant.ofEpochMilli(instant));
          }
          stream = stream.addElements(messages.get(i));
        }
        testStream = stream.advanceWatermarkToInfinity();
      }
      input.endArray();
      input.close();
      this.setInputTSData(testStream);
      return this;
    }

    public abstract Builder setOutputTSType1Data(KV<TSKey, TSDataPoint> output1);

    public abstract Builder setOutputTSType2Data(KV<TSKey, TSDataPoint> output2);

    public abstract TSTestData build();
  }
}
