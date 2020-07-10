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

import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

public class Dimensions {
  @DefaultSchema(JavaFieldSchema.class)
  public static class StoreLocation {
    public Integer id;
    public Integer zip;
    public String city, state;
    public Double lat, lng;

    @Override
    public boolean equals(Object obj) {

      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      final StoreLocation other = (StoreLocation) obj;
      return Objects.equals(this.id, other.id)
          && Objects.equals(this.zip, other.zip)
          && Objects.equals(this.city, other.city)
          && Objects.equals(this.state, other.state)
          && Objects.equals(this.lat, other.lat)
          && Objects.equals(this.lng, other.lng);
    }

    @Override
    public int hashCode() {

      return Objects.hash(this.id, this.zip, this.city, this.state, this.lat, this.lng);
    }
  }
}
