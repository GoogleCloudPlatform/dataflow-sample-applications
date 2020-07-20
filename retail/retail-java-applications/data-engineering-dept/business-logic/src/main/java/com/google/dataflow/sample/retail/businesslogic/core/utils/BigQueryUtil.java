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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This helper allows for reading of data from BigQuery from within a DoFn. It makes use of the
 * newer BigQuery client than that used by BigQueryIO.
 *
 * <p>TODO: Beam support a cleaner pattern for slow update sideinput, which once implemented in this
 * app will remove the need for this helper.
 */
@Experimental
public class BigQueryUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtil.class);

  public static TableResult readDataFromBigQueryUsingQueryString(
      String project, String sqlStatement, @Nullable String jobIdPrefix) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(project).build().getService();

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlStatement).setUseLegacySql(false).build();

    // Create a job ID so that we can safely retry.
    JobId jobId =
        JobId.of(
            String.format(
                "%s-%s",
                Optional.ofNullable(jobIdPrefix).orElse(""), UUID.randomUUID().toString()));
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    try {
      queryJob = queryJob.waitFor();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for BigQuery Job result.", e);
    }

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }
    TableResult result = null;
    try {
      result = queryJob.getQueryResults();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for BigQuery Job result.", e);
    }

    return result;
  }
}
