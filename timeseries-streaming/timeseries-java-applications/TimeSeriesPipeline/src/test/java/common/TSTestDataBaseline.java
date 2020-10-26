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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class TSTestDataBaseline {
  public static final TSKey KEY_A_A =
      TSKey.newBuilder().setMajorKey("Key-A").setMinorKeyString("MKey-a").build();
  public static final TSKey KEY_A_B =
      TSKey.newBuilder().setMajorKey("Key-A").setMinorKeyString("MKey-b").build();
  public static final TSKey KEY_A_C =
      TSKey.newBuilder().setMajorKey("Key-A").setMinorKeyString("MKey-c").build();
  public static final TSKey KEY_B_A =
      TSKey.newBuilder().setMajorKey("Key-B").setMinorKeyString("MKey-a").build();
  public static final TSKey KEY_B_B =
      TSKey.newBuilder().setMajorKey("Key-B").setMinorKeyString("MKey-b").build();
  public static final TSKey KEY_B_C =
      TSKey.newBuilder().setMajorKey("Key-B").setMinorKeyString("MKey-c").build();
  public static final TSKey KEY_C_A =
      TSKey.newBuilder().setMajorKey("Key-C").setMinorKeyString("MKey-a").build();
  public static final TSKey KEY_C_B =
      TSKey.newBuilder().setMajorKey("Key-C").setMinorKeyString("MKey-b").build();
  public static final TSKey KEY_C_C =
      TSKey.newBuilder().setMajorKey("Key-C").setMinorKeyString("MKey-c").build();
  public static final TimeSeriesData.TSDataPoint DATA_0 =
      TimeSeriesData.TSDataPoint.newBuilder().setData(CommonUtils.createNumData(0d)).build();
  public static final TimeSeriesData.TSDataPoint DATA_NEGATIVE_1 =
      TimeSeriesData.TSDataPoint.newBuilder().setData(CommonUtils.createNumData(-1d)).build();
  public static final TimeSeriesData.TSDataPoint DATA_1 =
      TimeSeriesData.TSDataPoint.newBuilder().setData(CommonUtils.createNumData(1d)).build();
  public static final TimeSeriesData.TSDataPoint DATA_2 =
      TimeSeriesData.TSDataPoint.newBuilder().setData(CommonUtils.createNumData(2d)).build();
  public static final TimeSeriesData.TSDataPoint DATA_3 =
      TimeSeriesData.TSDataPoint.newBuilder().setData(CommonUtils.createNumData(3d)).build();
  public static final Long START = Instant.parse("2000-01-01T00:00:00Z").getMillis();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_1_B_A =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_1)
          .setKey(KEY_B_A)
          .setTimestamp(Timestamps.fromMillis(START))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_1_A_C =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_1)
          .setKey(KEY_A_C)
          .setTimestamp(Timestamps.fromMillis(START))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_1_A_B =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_1)
          .setKey(KEY_A_B)
          .setTimestamp(Timestamps.fromMillis(START))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_1_A_A =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_1)
          .setKey(KEY_A_A)
          .setTimestamp(Timestamps.fromMillis(START))
          .build();
  public static final Timestamp PLUS_TEN_SECS_TIMESTAMP =
      Timestamps.fromMillis(
          Instant.ofEpochMilli(START).plus(Duration.standardSeconds(10)).getMillis());
  public static final Timestamp PLUS_FIVE_SECS_TIMESTAMP =
      Timestamps.fromMillis(
          Instant.ofEpochMilli(START).plus(Duration.standardSeconds(5)).getMillis());
  public static final Long PLUS_TEN_SECS =
      Instant.ofEpochMilli(START).plus(Duration.standardSeconds(10)).getMillis();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_3_B_B =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_3)
          .setKey(KEY_B_B)
          .setTimestamp(Timestamps.fromMillis(PLUS_TEN_SECS))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_3_A_C =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_3)
          .setKey(KEY_A_C)
          .setTimestamp(Timestamps.fromMillis(PLUS_TEN_SECS))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_3_A_B =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_3)
          .setKey(KEY_A_B)
          .setTimestamp(Timestamps.fromMillis(PLUS_TEN_SECS))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_3_A_A =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_3)
          .setKey(KEY_A_A)
          .setTimestamp(Timestamps.fromMillis(PLUS_TEN_SECS))
          .build();
  public static final Long PLUS_FIVE_SECS =
      Instant.ofEpochMilli(START).plus(Duration.standardSeconds(5)).getMillis();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_2_B_C =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_2)
          .setKey(KEY_A_C)
          .setTimestamp(Timestamps.fromMillis(PLUS_FIVE_SECS))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_2_B_A =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_2)
          .setKey(KEY_A_B)
          .setTimestamp(Timestamps.fromMillis(PLUS_FIVE_SECS))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_2_A_C =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_2)
          .setKey(KEY_A_C)
          .setTimestamp(Timestamps.fromMillis(PLUS_FIVE_SECS))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_2_A_B =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_2)
          .setKey(KEY_A_B)
          .setTimestamp(Timestamps.fromMillis(PLUS_FIVE_SECS))
          .build();
  public static final TimeSeriesData.TSDataPoint DOUBLE_POINT_2_A_A =
      TimeSeriesData.TSDataPoint.newBuilder(DATA_2)
          .setKey(KEY_A_A)
          .setTimestamp(Timestamps.fromMillis(PLUS_FIVE_SECS))
          .build();
  public static final Timestamp START_TIMESTAMP =
      Timestamps.fromMillis(Instant.parse("2000-01-01T00:00:00Z").getMillis());
}
