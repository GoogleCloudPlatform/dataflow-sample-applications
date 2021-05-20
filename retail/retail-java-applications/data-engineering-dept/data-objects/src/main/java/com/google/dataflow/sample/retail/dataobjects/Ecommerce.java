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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class Ecommerce {

  @SchemaFieldName("items")
  public @Nullable abstract List<Item> getItems();

  @SchemaFieldName("purchase")
  public @Nullable abstract Purchase getPurchase();

  public static Builder builder() {

    return new AutoValue_Ecommerce.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setItems(List<Item> value);

    public abstract Builder setPurchase(Purchase purchase);

    public abstract Ecommerce build();
  }
}
