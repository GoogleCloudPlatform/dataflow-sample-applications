/**
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

variable "project_id" {
  type        = string
  description = "Project ID in GCP"
}

variable "topic_clickstream_inbound" {
  type    = string
}

variable "topic_transactions_inbound" {
  type    = string
}

variable "topic_inventory_inbound" {
  type    = string
}

variable "topic_inventory_outbound" {
  type    = string
}

variable "clickstream_inbound_sub" {
  type    = string
}

variable "transactions_inbound_sub" {
  type    = string
}

variable "inventory_inbound_sub" {
  type    = string
}
