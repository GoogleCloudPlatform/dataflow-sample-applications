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
package com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.CMEAdapter.SSCLTOBJsonTransform;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.CMEAdapter.SSCLTRDJsonTransform;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CMEAdapterTest {

  public static final Logger LOG = LoggerFactory.getLogger(CMEAdapterTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // Create couple of test records.
  final String recordOneTob =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"OBTS\",\n"
          + "        \"sentTime\": \"2020-08-04T17:07:51.382813700\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"tradingStatus\": \"ReadyToTrade\",\n"
          + "        \"instrument\": {\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XCME\",\n"
          + "            \"id\": \"16337\",\n"
          + "            \"marketSegmentId\": \"68\",\n"
          + "            \"periodCode\": \"202012\",\n"
          + "            \"productCode\": \"NQ\",\n"
          + "            \"productGroup\": \"NQ\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"symbol\": \"NQZ0\"\n"
          + "        },\n"
          + "        \"askLevel\": [{\n"
          + "            \"lastUpdateTime\": \"2020-08-04T17:07:51.382681973\",\n"
          + "            \"orderCnt\": 1,\n"
          + "            \"price\": \"1105500\",\n"
          + "            \"qty\": 1\n"
          + "        }],\n"
          + "        \"bidLevel\": [{\n"
          + "            \"lastUpdateTime\": \"2020-08-04T17:07:51.382681973\",\n"
          + "            \"orderCnt\": 1,\n"
          + "            \"price\": \"1105175\",\n"
          + "            \"qty\": 1\n"
          + "        }]\n"
          + "    }]\n"
          + "}";

  final String recordTwoTob =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"OBTS\",\n"
          + "        \"sentTime\": \"2021-02-11T12:34:12.543995002Z\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"tradingStatus\": \"ReadyToTrade\",\n"
          + "        \"instrument\": {\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XCME\",\n"
          + "            \"id\": \"16908\",\n"
          + "            \"marketSegmentId\": \"68\",\n"
          + "            \"periodCode\": \"202006\",\n"
          + "            \"productCode\": \"NQ\",\n"
          + "            \"productGroup\": \"NQ\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"symbol\": \"NQM0\"\n"
          + "        },\n"
          + "        \"askLevel\": [{\n"
          + "            \"lastUpdateTime\": \"2021-02-11T12:34:12.543995000Z\",\n"
          + "            \"orderCnt\": 0,\n"
          + "            \"price\": \"940725\",\n"
          + "            \"qty\": 1\n"
          + "        }],\n"
          + "        \"bidLevel\": [{\n"
          + "            \"lastUpdateTime\": \"2021-02-11T12:34:12.543995000Z\",\n"
          + "            \"orderCnt\": 0,\n"
          + "            \"price\": \"940625\",\n"
          + "            \"qty\": 3\n"
          + "        }]\n"
          + "    }]\n"
          + "}";

  final String recordBadTob =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"OBTS\",\n"
          + "        \"sentTime\": \"2020-08-04T17:07:51.382813700\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"tradingStatus\": \"ReadyToTrade\",\n"
          + "        \"instrument\": {\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XCME\",\n"
          + "            \"id\": \"16908\",\n"
          + "            \"marketSegmentId\": \"68\",\n"
          + "            \"periodCode\": \"202006\",\n"
          + "            \"productCode\": \"NQ\",\n"
          + "            \"productGroup\": \"NQ\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"symbol\": \"NQM0\"\n"
          + "        },\n"
          + "        \"askLevel\": {\n"
          + "            \"lastUpdateTime\": \"2020-05-28T08:31:24.448688835\",\n"
          + "            \"orderCnt\": 0,\n"
          + "            \"price\": \"940725\",\n"
          + "            \"qty\": 1\n"
          + "        },\n"
          + "        \"bidLevel\": {\n"
          + "            \"lastUpdateTime\": \"2020-05-28T08:31:24.448688835\",\n"
          + "            \"orderCnt\": 0,\n"
          + "            \"price\": \"940625\",\n"
          + "            \"qty\": 3\n"
          + "        }\n"
          + "    }]\n"
          + "}";

  final String recordTobWithNulls =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"OBTS\",\n"
          + "        \"sentTime\": \"2020-08-04T17:07:51.382813700Z\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"tradingStatus\": \"ReadyToTrade\",\n"
          + "        \"instrument\": {\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XCME\",\n"
          + "            \"id\": \"16337\",\n"
          + "            \"marketSegmentId\": \"68\",\n"
          + "            \"periodCode\": \"202012\",\n"
          + "            \"productCode\": \"NQ\",\n"
          + "            \"productGroup\": \"NQ\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"symbol\": \"NQZ0\"\n"
          + "        },\n"
          + "        \"askLevel\": [{\n"
          + "            \"lastUpdateTime\": \"2020-08-04T17:07:51.382681973Z\",\n"
          + "            \"orderCnt\": 1,\n"
          + "            \"price\": null,\n"
          + "            \"qty\": 1\n"
          + "        }],\n"
          + "        \"bidLevel\": [{\n"
          + "            \"lastUpdateTime\": \"2020-08-04T17:07:51.382681973Z\",\n"
          + "            \"orderCnt\": 1,\n"
          + "            \"price\": \"1105175\",\n"
          + "            \"qty\": 1\n"
          + "        }]\n"
          + "    }]\n"
          + "}";

  @Test
  public void testSSCLTOBJsonTransform() {

    // Test Records Array
    final List<String> expected = new ArrayList<>();
    // Add test records in expected List for comparison
    expected.add(recordOneTob);
    expected.add(recordTwoTob);
    expected.add(recordBadTob);
    expected.add(recordTobWithNulls);

    // Test Records Count
    final long expectedCount = 2 * 11; // 2 valid input JSON records each with 11 TSDataPoints

    // Specify actual array list to store the final output elements
    List<String> actual = new ArrayList<>();

    // Convert JSON records to TSDataPoint
    PCollection<TSDataPoint> tobTSDataPoint =
        pipeline
            .apply(Create.of(expected).withCoder(StringUtf8Coder.of()))
            .apply(
                SSCLTOBJsonTransform.newBuilder()
                    .setDeadLetterSinkType(DeadLetterSink.LOG)
                    .setBigQueryDeadLetterSinkProject(null)
                    .setBigQueryDeadLetterSinkTable(null)
                    .build());

    // Log TSDataPoints
    tobTSDataPoint
        // Log to check output during test run
        .apply("Log TSDataPoints", new LogElements<TSDataPoint>());

    // Count the actual records to compare with expected count
    PCollection<Long> recordsCount = tobTSDataPoint.apply(Count.<TSDataPoint>globally());

    // Inspect the pipeline's results
    PAssert.thatSingleton(recordsCount).isEqualTo(expectedCount);

    // Execute the TestPipeline.
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSSCLTOBJsonTransformIgnoreCategorical() {

    // Test Records Array
    final List<TimestampedValue<String>> expected = new ArrayList<>();
    // Add test records in expected List for comparison
    expected.add(TimestampedValue.of(recordOneTob, Instant.parse("2020-08-04T17:07:51.382681973")));
    expected.add(
        TimestampedValue.of(recordTwoTob, Instant.parse("2021-02-11T12:34:12.543995000Z")));
    expected.add(TimestampedValue.of(recordBadTob, Instant.parse("2020-08-04T17:07:51.382813700")));
    expected.add(
        TimestampedValue.of(recordTobWithNulls, Instant.parse("2020-08-04T17:07:51.382681973Z")));

    // Test Records Count
    final long expectedCount =
        2 * 11 - (2 * 5); // 2 valid input JSON records each with 11 TSDataPoints 5 categorical

    // Specify actual array list to store the final output elements
    List<String> actual = new ArrayList<>();

    // Convert JSON records to TSDataPoint
    PCollection<TSDataPoint> tobTSDataPoint =
        pipeline
            .apply(Create.timestamped(expected).withCoder(StringUtf8Coder.of()))
            .apply(
                SSCLTOBJsonTransform.newBuilder()
                    .setSuppressCategorical(true)
                    .setDeadLetterSinkType(DeadLetterSink.LOG)
                    .setBigQueryDeadLetterSinkProject(null)
                    .setBigQueryDeadLetterSinkTable(null)
                    .build());

    // Count the actual records to compare with expected count
    PCollection<Long> recordsCount = tobTSDataPoint.apply(Count.<TSDataPoint>globally());

    // Inspect the pipeline's results
    PAssert.thatSingleton(recordsCount).isEqualTo(expectedCount);

    // Execute the TestPipeline.
    pipeline.run().waitUntilFinish();
  }

  // Create couple of test records.
  final String recordOneTrd =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"TS\",\n"
          + "        \"sentTime\": \"null\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"lastUpdateTime\": \"2020-10-19T20:18:44.872357987\",\n"
          + "        \"tradeSummary\": {\n"
          + "            \"aggressorSide\": \"1\",\n"
          + "            \"mdTradeEntryId\": \"11323435\",\n"
          + "            \"tradePrice\": \"4094\",\n"
          + "            \"tradeQty\": \"1\",\n"
          + "            \"tradeOrderCount\": \"2\",\n"
          + "            \"tradeUpdateAction\": \"NEW\",\n"
          + "            \"orderQty\": {\n"
          + "                \"orderId\": \"2\",\n"
          + "                \"lastOrdQty\": \"1\"\n"
          + "            }\n"
          + "        },\n"
          + "        \"instrument\": {\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XNYM\",\n"
          + "            \"id\": \"207193\",\n"
          + "            \"marketSegmentId\": \"80\",\n"
          + "            \"periodCode\": \"202012\",\n"
          + "            \"productCode\": \"CL\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"productGroup\": \"CL\",\n"
          + "            \"symbol\": \"CLZ0\"\n"
          + "        }\n"
          + "    }]\n"
          + "}";

  final String recordTwoTrd =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"TS\",\n"
          + "        \"sentTime\": \"null\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"lastUpdateTime\": \"2021-02-11T12:34:12.543995000Z\",\n"
          + "        \"tradeSummary\": {\n"
          + "            \"aggressorSide\": \"1\",\n"
          + "            \"mdTradeEntryId\": \"11323435\",\n"
          + "            \"tradePrice\": \"4094\",\n"
          + "            \"tradeQty\": \"1\",\n"
          + "            \"tradeOrderCount\": \"2\",\n"
          + "            \"tradeUpdateAction\": \"NEW\",\n"
          + "            \"orderQty\": {\n"
          + "                \"orderId\": \"2\",\n"
          + "                \"lastOrdQty\": \"1\"\n"
          + "            }\n"
          + "        },\n"
          + "        \"instrument\": {\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XNYM\",\n"
          + "            \"id\": \"207193\",\n"
          + "            \"marketSegmentId\": \"80\",\n"
          + "            \"periodCode\": \"202012\",\n"
          + "            \"productCode\": \"CL\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"productGroup\": \"CL\",\n"
          + "            \"symbol\": \"CLZ0\"\n"
          + "        }\n"
          + "    }]\n"
          + "}";

  final String recordBadTrd =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"TS\",\n"
          + "        \"sentTime\": \"null\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"lastUpdateTime\": \"2020-10-19T20:18:44.872357987\",\n"
          + "        \"tradeSummary\": {\n"
          + "            \"aggressorSide\": \"1\",\n"
          + "            \"mdTradeEntryId\": \"11323435\",\n"
          + "            \"tradePrice\": \"4094\",\n"
          + "            \"tradeQty\": \"1\",\n"
          + "            \"tradeOrderCount\": \"2\",\n"
          + "            \"tradeUpdateAction\": \"NEW\",\n"
          + "            \"orderQty\": {\n"
          + "                \"orderId\": \"2\",\n"
          + "                \"lastOrdQty\": \"1\"\n"
          + "            }\n"
          + "        },\n"
          + "        \"instrument\": [{\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XNYM\",\n"
          + "            \"id\": \"207193\",\n"
          + "            \"marketSegmentId\": \"80\",\n"
          + "            \"periodCode\": \"202012\",\n"
          + "            \"productCode\": \"CL\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"productGroup\": \"CL\",\n"
          + "            \"symbol\": \"CLZ0\"\n"
          + "        }]\n"
          + "    }]\n"
          + "}";

  final String recordTrdWithNulls =
      "{\n"
          + "    \"header\": {\n"
          + "        \"messageType\": \"TS\",\n"
          + "        \"sentTime\": \"null\",\n"
          + "        \"version\": \"1.0\"\n"
          + "    },\n"
          + "    \"payload\": [{\n"
          + "        \"lastUpdateTime\": \"2020-10-21T20:18:44.872357987Z\",\n"
          + "        \"tradeSummary\": {\n"
          + "            \"aggressorSide\": \"1\",\n"
          + "            \"mdTradeEntryId\": \"11323435\",\n"
          + "            \"tradePrice\": \"4094\",\n"
          + "            \"tradeQty\": null,\n"
          + "            \"tradeOrderCount\": \"2\",\n"
          + "            \"tradeUpdateAction\": \"NEW\",\n"
          + "            \"orderQty\": {\n"
          + "                \"orderId\": \"2\",\n"
          + "                \"lastOrdQty\": \"1\"\n"
          + "            }\n"
          + "        },\n"
          + "        \"instrument\": {\n"
          + "            \"definitionSource\": \"E\",\n"
          + "            \"exchangeMic\": \"XNYM\",\n"
          + "            \"id\": \"207193\",\n"
          + "            \"marketSegmentId\": \"80\",\n"
          + "            \"periodCode\": \"202012\",\n"
          + "            \"productCode\": \"CL\",\n"
          + "            \"productType\": \"FUT\",\n"
          + "            \"productGroup\": \"CL\",\n"
          + "            \"symbol\": \"CLZ0\"\n"
          + "        }\n"
          + "    }]\n"
          + "}";

  @Test
  public void testSSCLTRDJsonTransform() {

    // Test Records Array
    final List<TimestampedValue<String>> expected = new ArrayList<>();
    // Add test records in expected List for comparison
    expected.add(TimestampedValue.of(recordOneTrd, Instant.parse("2020-08-04T17:07:51.382681973")));
    expected.add(
        TimestampedValue.of(recordTwoTrd, Instant.parse("2021-02-11T12:34:12.543995000Z")));
    expected.add(TimestampedValue.of(recordBadTrd, Instant.parse("2020-08-04T17:07:51.382813700")));
    expected.add(
        TimestampedValue.of(recordTrdWithNulls, Instant.parse("2020-08-04T17:07:51.382681973Z")));

    // Test Records Count
    final long expectedCount = 2 * 8; // 2 valid input JSON records each with 8 TSDataPoints

    // Specify actual array list to store the final output elements
    List<String> actual = new ArrayList<>();

    // Convert JSON records to TSDataPoint
    PCollection<TSDataPoint> trdTSDataPoint =
        pipeline
            .apply(Create.timestamped(expected).withCoder(StringUtf8Coder.of()))
            .apply(
                SSCLTRDJsonTransform.newBuilder()
                    .setDeadLetterSinkType(DeadLetterSink.LOG)
                    .setBigQueryDeadLetterSinkProject(null)
                    .setBigQueryDeadLetterSinkTable(null)
                    .build());

    // Log TSDataPoints
    trdTSDataPoint
        // Log to check output during test run
        .apply("Log TSDataPoints", new LogElements<TSDataPoint>());

    // Count the actual records to compare with expected count
    PCollection<Long> recordsCount = trdTSDataPoint.apply(Count.<TSDataPoint>globally());

    // Inspect the pipeline's results
    PAssert.thatSingleton(recordsCount).isEqualTo(expectedCount);

    // Execute the TestPipeline.
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSSCLTRDJsonTransformIgnoreCategorical() {

    // Test Records Array
    final List<TimestampedValue<String>> expected = new ArrayList<>();
    // Add test records in expected List for comparison
    expected.add(TimestampedValue.of(recordOneTrd, Instant.parse("2020-08-04T17:07:51.382681973")));
    expected.add(
        TimestampedValue.of(recordTwoTrd, Instant.parse("2021-02-11T12:34:12.543995000Z")));
    expected.add(TimestampedValue.of(recordBadTrd, Instant.parse("2020-08-04T17:07:51.382813700")));
    expected.add(
        TimestampedValue.of(recordTrdWithNulls, Instant.parse("2020-08-04T17:07:51.382681973Z")));

    // Test Records Count
    final long expectedCount =
        2 * 8 - (2 * 5); // 2 valid input JSON records each with 8 TSDataPoints, 5 categorical

    // Specify actual array list to store the final output elements
    List<String> actual = new ArrayList<>();

    // Convert JSON records to TSDataPoint
    PCollection<TSDataPoint> trdTSDataPoint =
        pipeline
            .apply(Create.timestamped(expected).withCoder(StringUtf8Coder.of()))
            .apply(
                SSCLTRDJsonTransform.newBuilder()
                    .setSuppressCategorical(true)
                    .setDeadLetterSinkType(DeadLetterSink.LOG)
                    .setBigQueryDeadLetterSinkProject(null)
                    .setBigQueryDeadLetterSinkTable(null)
                    .build());

    // Count the actual records to compare with expected count
    PCollection<Long> recordsCount = trdTSDataPoint.apply(Count.<TSDataPoint>globally());

    // Inspect the pipeline's results
    PAssert.thatSingleton(recordsCount).isEqualTo(expectedCount);

    // Execute the TestPipeline.
    pipeline.run().waitUntilFinish();
  }
}
