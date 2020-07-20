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
import com.google.dataflow.sample.retail.dataobjects.Dimensions.StoreLocation;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

/** A transaction is a purchase, either in-store or via the website / mobile application. */
@Experimental
public class Transaction {

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class TransactionEvent {
    @SchemaFieldName("timestamp")
    public abstract @Nullable Long getTimestamp();

    @SchemaFieldName("uid")
    public abstract @Nullable Integer getUid();

    @SchemaFieldName("order_number")
    public abstract @Nullable String getOrderNumber();

    @SchemaFieldName("user_id")
    public abstract @Nullable Integer getUserId();

    @SchemaFieldName("store_id")
    public abstract @Nullable Integer getStoreId();

    @SchemaFieldName("time_of_sale")
    public abstract @Nullable Long getTimeOfSale();

    @SchemaFieldName("department_id")
    public abstract @Nullable Integer getDepartmentId();

    @SchemaFieldName("product_id")
    public abstract @Nullable Integer getProductId();

    @SchemaFieldName("product_count")
    public abstract @Nullable Integer getProductCount();

    @SchemaFieldName("price")
    public abstract @Nullable Float getPrice();

    public abstract @Nullable StoreLocation getStoreLocation();

    public abstract TransactionEvent.Builder toBuilder();

    public static TransactionEvent.Builder builder() {
      return new AutoValue_Transaction_TransactionEvent.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTimestamp(Long value);

      public abstract Builder setUid(Integer value);

      public abstract Builder setOrderNumber(String value);

      public abstract Builder setUserId(Integer value);

      public abstract Builder setStoreId(Integer value);

      public abstract Builder setTimeOfSale(Long value);

      public abstract Builder setDepartmentId(Integer value);

      public abstract Builder setProductId(Integer value);

      public abstract Builder setProductCount(Integer value);

      public abstract Builder setPrice(Float value);

      public abstract Builder setStoreLocation(StoreLocation value);

      public abstract TransactionEvent build();
    }
  }
}
