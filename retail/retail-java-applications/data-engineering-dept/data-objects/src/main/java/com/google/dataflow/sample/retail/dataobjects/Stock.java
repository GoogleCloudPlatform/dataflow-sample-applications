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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

/**
 * A Inventory event is linked to a purchase, either in-store or via the website / mobile
 * application, or a delivery.
 */
@Experimental
public class Stock {

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class StockEvent {
    public abstract @Nullable Integer getCount();

    public abstract @Nullable Integer getSku();

    @SchemaFieldName("product_id")
    public abstract @Nullable Integer getProductId();

    @SchemaFieldName("store_id")
    public abstract @Nullable Integer getStoreId();

    public abstract @Nullable Integer getAisleId();

    public abstract @Nullable String getProduct_name();

    public abstract @Nullable Integer getDepartmentId();

    public abstract @Nullable Float getPrice();

    public abstract @Nullable String getRecipeId();

    public abstract @Nullable String getImage();

    public abstract @Nullable Long getTimestamp();

    public abstract Stock.StockEvent.Builder toBuilder();

    public static Stock.StockEvent.Builder builder() {

      return new AutoValue_Stock_StockEvent.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setCount(Integer value);

      public abstract Builder setSku(Integer value);

      public abstract Builder setProductId(Integer value);

      public abstract Builder setStoreId(Integer value);

      public abstract Builder setAisleId(Integer value);

      public abstract Builder setProduct_name(String value);

      public abstract Builder setDepartmentId(Integer value);

      public abstract Builder setPrice(Float value);

      public abstract Builder setRecipeId(String value);

      public abstract Builder setImage(String value);

      public abstract Stock.StockEvent.Builder setTimestamp(Long value);

      public abstract StockEvent build();
    }
  }
}
