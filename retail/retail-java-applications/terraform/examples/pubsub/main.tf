/**
 * Copyright 2021 Google LLC
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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 3.48.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "random_string" "resource_name_unique" {
  count     = 7
  length    = 5
  lower     = true
  min_lower = 5
}

module "pubsub" {
  source                     = "../../modules/pubsub"
  topic_clickstream_inbound  = join("-", [var.topic_clickstream_inbound, random_string.resource_name_unique.0.id])
  topic_transactions_inbound = join("-", [var.topic_transactions_inbound, random_string.resource_name_unique.1.id])
  topic_inventory_inbound    = join("-", [var.topic_inventory_inbound, random_string.resource_name_unique.2.id])
  topic_inventory_outbound   = join("-", [var.topic_inventory_outbound, random_string.resource_name_unique.3.id])
  clickstream_inbound_sub    = join("-", [var.clickstream_inbound_sub, random_string.resource_name_unique.4.id])
  transactions_inbound_sub   = join("-", [var.transactions_inbound_sub, random_string.resource_name_unique.5.id])
  inventory_inbound_sub      = join("-", [var.inventory_inbound_sub, random_string.resource_name_unique.6.id])
}

output "clickstream_inbound_topic" {
  value = module.pubsub.clickstream_inbound_topic
}

output "transactions_inbound_topic" {
  value = module.pubsub.transactions_inbound_topic
}

output "inventory_inbound_topic" {
  value = module.pubsub.inventory_inbound_topic
}

output "inventory_outbound_topic" {
  value = module.pubsub.inventory_outbound_topic
}

output "clickstream_inbound_sub" {
  value = module.pubsub.clickstream_inbound_sub
}

output "transactions_inbound_sub" {
  value = module.pubsub.transactions_inbound_sub
}

output "inventory_inbound_sub" {
  value = module.pubsub.inventory_inbound_sub
}
