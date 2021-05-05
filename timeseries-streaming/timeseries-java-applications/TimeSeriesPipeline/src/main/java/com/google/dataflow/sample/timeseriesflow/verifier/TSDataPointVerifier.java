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
package com.google.dataflow.sample.timeseriesflow.verifier;

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ensure all TSDataPoint is valid. */
@Experimental
@AutoValue
public abstract class TSDataPointVerifier
    extends PTransform<PCollection<TSDataPoint>, PCollection<TSDataPoint>> {

  public abstract boolean getAllowIncorrectTimeMismatch();

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAllowIncorrectTimeMismatch(boolean allowIncorrectTimeMismatch);

    public abstract TSDataPointVerifier build();
  }

  public static TSDataPointVerifier create() {
    return TSDataPointVerifier.builder().setAllowIncorrectTimeMismatch(false).build();
  }

  public TSDataPointVerifier allowSourceDataTimeMismatch() {
    return this.toBuilder().setAllowIncorrectTimeMismatch(true).build();
  }

  public static Builder builder() {
    return new AutoValue_TSDataPointVerifier.Builder();
  }

  private static final Logger LOG = LoggerFactory.getLogger(TSDataPointVerifier.class);

  @Override
  public PCollection<TSDataPoint> expand(PCollection<TSDataPoint> input) {
    return input.apply(ParDo.of(new VerifyTSDataPoint(this)));
  }

  private static class VerifyTSDataPoint extends DoFn<TSDataPoint, TSDataPoint> {

    final TSDataPointVerifier tsDataPointVerifier;

    private VerifyTSDataPoint(TSDataPointVerifier tsDataPointVerifier) {
      this.tsDataPointVerifier = tsDataPointVerifier;
    }

    @ProcessElement
    public void process(
        @Element TSDataPoint input, @Timestamp Instant time, OutputReceiver<TSDataPoint> o) {

      if (!(input.hasData() && CommonUtils.hasData(input.getData()))) {
        throw new IllegalStateException(
            String.format("TSDataPoint has no data. %s", input.toString()));
      }

      // Categorical values are not yet supported.
      if (!(input.getData().getCategoricalVal().isEmpty())) {
        throw new IllegalStateException(
            String.format("Categorical Values are not yet supported. %s", input.toString()));
      }

      if (!(input.hasKey())) {
        throw new IllegalStateException(
            String.format("TSDataPoint has no key. %s", input.toString()));
      }

      if (!(input.hasTimestamp())) {
        throw new IllegalStateException(
            String.format("TSDataPoint has no timestamp. %s", input.toString()));
      }

      if (!(time.isAfter(GlobalWindow.TIMESTAMP_MIN_VALUE)
          && time.isBefore(GlobalWindow.TIMESTAMP_MAX_VALUE))) {
        throw new IllegalStateException(
            String.format(
                "Detected TSDataPoints which are at min or max values for the Global Window. "
                    + "The library expects TSDataPoints with timestamps larger than GlobalWindow.TIMESTAMP_MIN_VALUE "
                    + "and smaller than GlobalWindow.TIMESTAMP_MAX_VALUE. The timestamp value was %s\"",
                time.toString()));
      }

      if (time.getMillis() != Timestamps.toMillis(input.getTimestamp())) {

        String errorMsg =
            String.format(
                "TSDataPoint %s timestamp %s does not match the Beam timestamp %s. "
                    + "Ensure you have configured your source to make use of Event time."
                    + "For example with PubSubIO make use of withTimestampAttribute()."
                    + "Without correcting this issue, values can end up in incorrect windows.",
                input.getKey().toString(),
                Timestamps.toMillis(input.getTimestamp()),
                time.getMillis());

        if (tsDataPointVerifier.getAllowIncorrectTimeMismatch()) {
          LOG.warn(errorMsg);
        } else {
          throw new IllegalStateException(
              errorMsg
                  + " Note although discouraged you can allow this behaviour by setting AllowIncorrectTimeMismatch");
        }
      }

      o.output(input);
    }
  }
}
