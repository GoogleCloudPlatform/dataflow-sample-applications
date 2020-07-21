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
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.transforms.Convert;
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
 * This transform creates sessions from the incoming clickstream using the sessionid and
 * SessionWindows.
 */
@Experimental
public class CreateClickStreamSessions
    extends PTransform<PCollection<ClickStreamEvent>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterSink.class);

  Duration sessionWindowGapDuration;

  public CreateClickStreamSessions(Duration sessionWindowGapDuration) {
    this.sessionWindowGapDuration = sessionWindowGapDuration;
  }

  public CreateClickStreamSessions(@Nullable String name, Duration sessionWindowGapDuration) {
    super(name);
    this.sessionWindowGapDuration = sessionWindowGapDuration;
  }

  public static CreateClickStreamSessions create(Duration sessionWindowGapDuration) {
    return new CreateClickStreamSessions(sessionWindowGapDuration);
  }

  @Override
  public PCollection<Row> expand(PCollection<ClickStreamEvent> input) {
    return input
        .apply(Window.into(Sessions.withGapDuration(sessionWindowGapDuration)))
        .apply(Convert.toRows())
        .apply(Group.byFieldNames("sessionId"));
  }
}
