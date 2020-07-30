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
package com.google.dataflow.sample.retail.businesslogic.core.utils;

import com.google.dataflow.sample.retail.businesslogic.core.DeploymentAnnotations.NoPartialResultsOnDrain;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.DeadLetterSink;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.DeadLetterSink.SinkType;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.ErrorMsg;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class JSONUtils {

  /**
   * Convert an object to a POJO, on failure send JSON String to DeadLetter output.
   *
   * <p>TODO convert to AutoValue for configuration.
   *
   * @param <T>
   */
  @NoPartialResultsOnDrain
  public static class ConvertJSONtoPOJO<T> extends PTransform<PCollection<String>, PCollection<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertJSONtoPOJO.class);

    public static <T> ConvertJSONtoPOJO<T> create(Class<T> t) {
      return new ConvertJSONtoPOJO<T>(t, null);
    }

    public static <T> ConvertJSONtoPOJO<T> create(Class<T> t, SinkType sinkType) {
      return new ConvertJSONtoPOJO<T>(t, sinkType);
    }

    Class<T> clazz;
    SinkType sinkType;

    public ConvertJSONtoPOJO(Class<T> clazz, SinkType sinkType) {
      this.clazz = clazz;
      this.sinkType = sinkType;
    }

    public ConvertJSONtoPOJO(@Nullable String name, Class<T> clazz, SinkType sinkType) {
      super(name);
      this.clazz = clazz;
      this.sinkType = sinkType;
    }

    @Override
    public PCollection<T> expand(PCollection<String> input) {
      Schema objectSchema = null, errMessageSchema = null;

      try {
        objectSchema = input.getPipeline().getSchemaRegistry().getSchema(clazz);
      } catch (NoSuchSchemaException e) {
        LOG.error(e.getMessage());
        throw new IllegalArgumentException(
            String.format("Could not find schema for Object of %s", clazz.getCanonicalName()));
      }

      try {
        errMessageSchema = input.getPipeline().getSchemaRegistry().getSchema(ErrorMsg.class);
      } catch (NoSuchSchemaException e) {
        LOG.error(e.getMessage());
        throw new IllegalArgumentException(
            String.format("Could not find schema for %s", ErrorMsg.class.getCanonicalName()));
      }

      ParseResult result =
          input.apply(JsonToRow.withExceptionReporting(objectSchema).withExtendedErrorInfo());

      // We need to deal with json strings that have failed to parse.

      PCollection<ErrorMsg> errorMsgs =
          result
              .getFailedToParseLines()
              .apply("CreateErrorMessages", ParDo.of(new CreateErrorEvents(errMessageSchema)))
              .setRowSchema(errMessageSchema)
              .apply("ConvertErrMsgToRows", Convert.fromRows(ErrorMsg.class));

      if (sinkType != null) {
        errorMsgs.apply(DeadLetterSink.createSink(sinkType));
      } else {
        // Always output parse issues, minimum area will be to logging.
        errorMsgs.apply(DeadLetterSink.createSink(SinkType.LOG));
      }
      // Convert the parsed results to the POJO using Convert operation.
      PCollection<Row> output = result.getResults();
      return output.apply("ConvertRowsToPOJO", Convert.fromRows(clazz));
    }
  }

  private static class CreateErrorEvents extends DoFn<Row, Row> {

    private static final String METRIC_NAMESPACE = "JsonConverstion";

    private static final String DEAD_LETTER_METRIC_NAME = "JSONParseFailure";

    Schema errMessage;

    private Distribution jsonConversionErrors =
        Metrics.distribution(METRIC_NAMESPACE, DEAD_LETTER_METRIC_NAME);

    public CreateErrorEvents(Schema errMessage) {
      this.errMessage = errMessage;
    }

    @ProcessElement
    public void processElement(
        @FieldAccess("line") String jsonString,
        @FieldAccess("err") String errorMessage,
        @Timestamp Instant timestamp,
        OutputReceiver<Row> o) {

      jsonConversionErrors.update(1L);

      System.out.println(errorMessage);

      o.output(
          Row.withSchema(errMessage)
              .withFieldValue("data", jsonString)
              .withFieldValue("error", errorMessage)
              .withFieldValue("timestamp", timestamp)
              .build());
    }
  }
}
