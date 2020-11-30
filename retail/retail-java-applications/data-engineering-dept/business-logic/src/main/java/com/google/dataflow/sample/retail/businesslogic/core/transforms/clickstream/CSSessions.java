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

import com.google.dataflow.sample.retail.businesslogic.core.transforms.DeadLetterSink;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transform creates sessions from the incoming clickstream using the cliendId and
 * SessionWindows.
 */
@Experimental
public class CSSessions extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterSink.class);

  Duration sessionWindowGapDuration;

  public CSSessions(Duration sessionWindowGapDuration) {
    this.sessionWindowGapDuration = sessionWindowGapDuration;
  }

  public CSSessions(@Nullable String name, Duration sessionWindowGapDuration) {
    super(name);
    this.sessionWindowGapDuration = sessionWindowGapDuration;
  }

  public static CSSessions create(Duration sessionWindowGapDuration) {
    return new CSSessions(sessionWindowGapDuration);
  }

  @Override
  /**
   * Returns a Row object in the format:
   *
   * <pre>{@code
   * Field Name	    Field Type
   * key	        ROW{clientID:STRING}
   * values	        ITERABLE[ROW[ClickstreamEvent]]
   * }</pre>
   */
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
        .apply(Window.into(Sessions.withGapDuration(sessionWindowGapDuration)))
        .apply(Group.byFieldNames("client_id"));
  }
}
