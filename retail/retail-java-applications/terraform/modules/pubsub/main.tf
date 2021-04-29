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
resource "google_project_service" "pubsub" {
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false
}

resource "google_pubsub_topic" "topic_clickstream_inbound" {
  name = var.topic_clickstream_inbound
  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_topic" "topic_transactions_inbound" {
  name = var.topic_transactions_inbound

  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_topic" "topic_inventory_inbound" {
  name = var.topic_inventory_inbound

  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_topic" "topic_inventory_outbound" {
  name = var.topic_inventory_outbound

  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_subscription" "clickstream_inbound_sub" {
  name  = var.clickstream_inbound_sub
  topic = google_pubsub_topic.topic_clickstream_inbound.name

  labels = {
    created = "terraform"
  }
  
  retain_acked_messages      = false

  ack_deadline_seconds       = 20

  enable_message_ordering    = false
}

resource "google_pubsub_subscription" "transactions_inbound_sub" {
  name  = var.transactions_inbound_sub
  topic = google_pubsub_topic.topic_transactions_inbound.name

  labels = {
    created = "terraform"
  }
  
  retain_acked_messages      = false

  ack_deadline_seconds       = 20

  enable_message_ordering    = false
}

resource "google_pubsub_subscription" "inventory_inbound_sub" {
  name  = var.inventory_inbound_sub
  topic = google_pubsub_topic.topic_inventory_inbound.name

  labels = {
    created = "terraform"
  }
  
  retain_acked_messages      = false

  ack_deadline_seconds       = 20

  enable_message_ordering    = false
}

output "clickstream_inbound_topic" {
  value = var.topic_clickstream_inbound
}

output "transactions_inbound_topic" {
  value = var.topic_transactions_inbound
}

output "inventory_inbound_topic" {
  value = var.topic_inventory_inbound
}

output "inventory_outbound_topic" {
  value = var.topic_inventory_outbound
}

output "clickstream_inbound_sub" {
  value = var.clickstream_inbound_sub
}

output "transactions_inbound_sub" {
  value = var.transactions_inbound_sub
}

output "inventory_inbound_sub" {
  value = var.inventory_inbound_sub
}
