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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

/**
 * Objects used for dealing with clickstream within the pipeline and schemas for I/O of clickstream
 * events.
 */
@Experimental
public class ClickStream {

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  /**
   * The Clickstream event represents actions that a user has taken on the website or mobile
   * application.
   */
  public abstract static class ClickStreamEvent {

    @SchemaFieldName("event_datetime")
    public @Nullable abstract String getEventTime();

    @SchemaFieldName("event")
    public @Nullable abstract String getEvent();

    @SchemaFieldName("timestamp")
    public @Nullable abstract Long getTimestamp();

    @SchemaFieldName("user_id")
    public @Nullable abstract Long getUid();

    @SchemaFieldName("sessionId")
    public @Nullable abstract String getSessionId();

    @SchemaFieldName("pageRef")
    public @Nullable abstract String getPageRef();

    @SchemaFieldName("pageTarget")
    public @Nullable abstract String getPageTarget();

    @SchemaFieldName("lat")
    public @Nullable abstract Double getLat();

    @SchemaFieldName("lng")
    public @Nullable abstract Double getLng();

    @SchemaFieldName("agent")
    public @Nullable abstract String getAgent();

    @SchemaFieldName("items")
    public @Nullable abstract List<Items> getItems();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_ClickStream_ClickStreamEvent.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setEventTime(String value);

      public abstract Builder setEvent(String value);

      public abstract Builder setTimestamp(Long value);

      public abstract Builder setUid(Long value);

      public abstract Builder setSessionId(String value);

      public abstract Builder setPageRef(String value);

      public abstract Builder setPageTarget(String value);

      public abstract Builder setLat(Double value);

      public abstract Builder setLng(Double value);

      public abstract Builder setAgent(String value);

      public abstract Builder setItems(List<Items> value);

      public abstract ClickStreamEvent build();
    }
  }

  // -----------------------------------
  // Schema used for dealing with page views when working with BigTable.
  // -----------------------------------

  /** This class hosts the strings used for the row being stored in BigTable. */
  public static class ClickStreamBigTableSchema {
    public static final String PAGE_VIEW_AGGREGATION_COL_FAMILY = "pageViewAgg";
    public static final String PAGE_VIEW_AGGREGATION_COL_PAGE_VIEW_REF = "pageViewRef";
    public static final String PAGE_VIEW_AGGREGATION_COL_PAGE_VIEW_COUNT = "pageViewCount";
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Items {

    @SchemaFieldName("item_name")
    public @Nullable abstract String getItemName();

    @SchemaFieldName("item_id")
    public @Nullable abstract String getItemId();

    @SchemaFieldName("price")
    public @Nullable abstract String getPrice();

    @SchemaFieldName("item_brand")
    public @Nullable abstract String getItemBrand();

    @SchemaFieldName("item_category_2")
    public @Nullable abstract String getItemCat02();

    @SchemaFieldName("item_category_3")
    public @Nullable abstract String getItemCat03();

    @SchemaFieldName("item_category_4")
    public @Nullable abstract String getItemCat04();

    @SchemaFieldName("item_category_5")
    public @Nullable abstract String getItemCat05();

    @SchemaFieldName("item_variant")
    public @Nullable abstract String getItemVariant();

    @SchemaFieldName("item_list_name")
    public @Nullable abstract String getItemListName();

    @SchemaFieldName("item_list_id")
    public @Nullable abstract String getItemListId();

    @SchemaFieldName("index")
    public @Nullable abstract String getIndex();

    @SchemaFieldName("quantity")
    public @Nullable abstract String getQuantity();

    public static Builder builder() {
      return new AutoValue_ClickStream_Items.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setItemName(String newItemName);

      public abstract Builder setItemId(String newItemId);

      public abstract Builder setPrice(String newPrice);

      public abstract Builder setItemBrand(String newItemBrand);

      public abstract Builder setItemCat02(String newItemCat02);

      public abstract Builder setItemCat03(String newItemCat03);

      public abstract Builder setItemCat04(String newItemCat04);

      public abstract Builder setItemCat05(String newItemCat05);

      public abstract Builder setItemVariant(String newItemVariant);

      public abstract Builder setItemListName(String newItemListName);

      public abstract Builder setItemListId(String newItemListId);

      public abstract Builder setIndex(String newIndex);

      public abstract Builder setQuantity(String newQuantity);

      public abstract Items build();
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PageViewAggregator {
    public @Nullable abstract Long getDurationMS();

    public @Nullable abstract Long getStartTime();

    public @Nullable abstract String getPageRef();

    public @Nullable abstract Long getCount();

    public abstract Builder toBuilder();

    public static Builder builder() {

      return new AutoValue_ClickStream_PageViewAggregator.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDurationMS(Long value);

      public abstract Builder setStartTime(Long value);

      public abstract Builder setPageRef(String value);

      public abstract Builder setCount(Long value);

      public abstract PageViewAggregator build();
    }
  }
}
