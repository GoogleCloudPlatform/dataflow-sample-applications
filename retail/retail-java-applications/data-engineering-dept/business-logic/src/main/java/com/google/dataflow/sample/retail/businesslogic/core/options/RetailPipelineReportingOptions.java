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
package com.google.dataflow.sample.retail.businesslogic.core.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface RetailPipelineReportingOptions extends PipelineOptions {

  @Description("Deadletter Table for pipeline.")
  String getDeadLetterTable();

  void setDeadLetterTable(String deadletterBigQueryTable);

  @Description("Project used for data warehousing.")
  String getDataWarehouseOutputProject();

  void setDataWarehouseOutputProject(String dataWarehouseOutputProject);

  @Default.String("Retail_Store")
  String getMainReportingDataset();

  void setMainReportingDataset(String mainReportingDataset);

  @Default.String("Retail_Store_Aggregations")
  String getAggregateBigQueryDataset();

  void setAggregateBigQueryDataset(String aggregateBigQueryDataset);

  @Default.String("/topics/global-stock-level-topic")
  String getAggregateStockPubSubOutputTopic();

  void setAggregateStockPubSubOutputTopic(String aggregateStockPubSubOutputTopic);

}
