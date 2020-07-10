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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.common.primitives.Longs;
import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamBigTableSchema;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.PageViewAggregator;
import java.io.UnsupportedEncodingException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.joda.time.Duration;

public class WriteAggregatesToBigTable {

  public static WriteToBigTable writeToBigTable(Duration windowSize) {
    return new WriteToBigTable(windowSize);
  }

  public static class WriteToBigTable extends PTransform<PCollection<PageViewAggregator>, PDone> {

    Duration windowSize;

    public WriteToBigTable(Duration windowSize) {
      this.windowSize = windowSize;
    }

    public WriteToBigTable(@Nullable String name, Duration windowSize) {
      super(name);
      this.windowSize = windowSize;
    }

    @Override
    public PDone expand(PCollection<PageViewAggregator> input) {

      RetailPipelineOptions options =
          input.getPipeline().getOptions().as(RetailPipelineOptions.class);

      CloudBigtableTableConfiguration tableConfiguration =
          new CloudBigtableTableConfiguration.Builder()
              .withProjectId(options.getProject())
              .withInstanceId(options.getAggregateBigTableInstance())
              .withTableId("PageView5MinAggregates")
              .build();

      return input
          .apply(
              "Convert Aggregation to BigTable Put",
              ParDo.of(new CreateBigTableRowFromAggregation()))
          .apply(CloudBigtableIO.writeToTable(tableConfiguration));
    }
  }

  public static class CreateBigTableRowFromAggregation extends DoFn<PageViewAggregator, Mutation> {
    @ProcessElement
    public void process(@Element PageViewAggregator input, OutputReceiver<Mutation> o)
        throws UnsupportedEncodingException {

      Put put = new Put(String.format("%s-%s", input.pageRef, input.startTime).getBytes("UTF-8"));

      String charset = "UTF-8";

      // TODO This should never be Null eliminate bug.
      String pageRef = Optional.ofNullable(input.pageRef).orElse("");
      Long count = Optional.ofNullable(input.count).orElse(0L);

      put.addColumn(
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_FAMILY.getBytes(charset),
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_PAGE_VIEW_REF.getBytes(charset),
          pageRef.getBytes(charset));

      put.addColumn(
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_FAMILY.getBytes(charset),
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_PAGE_VIEW_COUNT.getBytes(charset),
          Longs.toByteArray(count));

      o.output(put);
    }
  }
}
