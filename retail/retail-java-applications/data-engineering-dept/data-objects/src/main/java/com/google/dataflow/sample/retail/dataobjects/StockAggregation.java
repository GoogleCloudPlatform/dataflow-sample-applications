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
package com.google.dataflow.sample.retail.dataobjects;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class StockAggregation {

  @Nullable
  public abstract Long getDurationMS();

  @Nullable
  public abstract Long getStartTime();

  @Nullable
  public abstract Integer getProductId();

  @Nullable
  public abstract Integer getStoreId();

  @Nullable
  public abstract Long getCount();

  public abstract StockAggregation.Builder toBuilder();

  public static StockAggregation.Builder builder() {
    return new AutoValue_StockAggregation.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDurationMS(Long value);

    public abstract Builder setStartTime(Long value);

    public abstract Builder setProductId(Integer value);

    public abstract Builder setStoreId(Integer value);

    public abstract Builder setCount(Long value);

    public abstract StockAggregation build();
  }
}
