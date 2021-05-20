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
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

/**
 *
 *
 * <pre>{@code
 * {
 *   "event_datetime":"2020-11-16 20:59:59",
 *   "event": "purchase",
 *   "user_id": "UID00001",
 *   "client_id": "CID00003",
 *   "page":"/checkout",
 *   "page_previous": "/order-confirmation",
 *   "ecommerce": {
 *     "purchase": {
 *       "transaction_id": "T12345",
 *       "affiliation": "Online Store",
 *       "value": 35.43,
 *       "tax": 4.90,
 *       "shipping": 5.99,
 *       "currency": "EUR",
 *       "coupon": "SUMMER_SALE",
 *       "items": [{
 *         "item_name": "Triblend Android T-Shirt",
 *         "item_id": "12345",
 *         "item_price": 15.25,
 *         "item_brand": "Google",
 *         "item_category": "Apparel",
 *         "item_variant": "Gray",
 *         "quantity": 1,
 *         "item_coupon": ""
 *       }, {
 *         "item_name": "Donut Friday Scented T-Shirt",
 *         "item_id": "67890",
 *         "item_price": 33.75,
 *         "item_brand": "Google",
 *         "item_category": "Apparel",
 *         "item_variant": "Black",
 *         "quantity": 1
 *       }]
 *     }
 *   }
 * }
 *
 * }</pre>
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class Purchase {

  @SchemaFieldName("transaction_id")
  public @Nullable abstract String getItemName();

  @SchemaFieldName("affiliation")
  public @Nullable abstract String getItemId();

  @SchemaFieldName("value")
  public @Nullable abstract String getPrice();

  @SchemaFieldName("tax")
  public @Nullable abstract String getItemBrand();

  @SchemaFieldName("shipping")
  public @Nullable abstract String getItemCat01();

  @SchemaFieldName("currency")
  public @Nullable abstract String getItemCat02();

  @SchemaFieldName("coupon")
  public @Nullable abstract String getItemCat03();

  public static Builder builder() {
    return new AutoValue_Purchase.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setItemName(String newItemName);

    public abstract Builder setItemId(String newItemId);

    public abstract Builder setPrice(String newPrice);

    public abstract Builder setItemBrand(String newItemBrand);

    public abstract Builder setItemCat01(String newItemCat01);

    public abstract Builder setItemCat02(String newItemCat02);

    public abstract Builder setItemCat03(String newItemCat03);

    public abstract Purchase build();
  }
}
