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
package com.google.dataflow.sample.retail.businesslogic.core.transforms;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

/** Error Objects for Dead Letter */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ErrorMsg {
  public @Nullable abstract String getTransform();

  public @Nullable abstract String getError();

  public @Nullable abstract String getData();

  public @Nullable abstract Instant getTimestamp();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_ErrorMsg.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTransform(String value);

    public abstract Builder setError(String value);

    public abstract Builder setData(String value);

    public abstract Builder setTimestamp(Instant value);

    public abstract ErrorMsg build();
  }
}
