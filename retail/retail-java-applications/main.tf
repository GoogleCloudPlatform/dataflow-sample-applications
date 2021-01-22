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

provider "google" {
  version = "3.48.0"

  project = "xl-project-302106"
  region  = "us-central1"
}

//enable pub/sub, bigquery and bigtable APIs

resource "google_project_service" "pubsub" {
  service = "pubsub.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  service = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigtable" {
  service = "bigtable.googleapis.com"
  disable_on_destroy = false
}

//create pub/sub, bigquery and bigtable resources
resource "google_pubsub_topic" "ps_topic_c_inbound" {
  name = "Clickstream-inbound"

  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_topic" "ps_topic_t_inbound" {
  name = "Transactions-inbound"

  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_topic" "ps_topic_i_inbound" {
  name = "Inventory-inbound"

  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_topic" "ps_topic_i_outbound" {
  name = "Inventory-outbound"

  labels = {
    created = "terraform"
  }

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_subscription" "ps_c_inbound_sub" {
  name  = "Clickstream-inbound-sub"
  topic = google_pubsub_topic.ps_topic_c_inbound.name

  labels = {
    created = "terraform"
  }
  
  retain_acked_messages      = false

  ack_deadline_seconds = 20

  enable_message_ordering    = false
}

resource "google_pubsub_subscription" "ps_t_inbound_sub" {
  name  = "Transactions-inbound-sub"
  topic = google_pubsub_topic.ps_topic_t_inbound.name

  labels = {
    created = "terraform"
  }
  
  retain_acked_messages      = false

  ack_deadline_seconds = 20

  enable_message_ordering    = false
}

resource "google_pubsub_subscription" "ps_i_inbound_sub" {
  name  = "Inventory-inbound-sub"
  topic = google_pubsub_topic.ps_topic_i_inbound.name

  labels = {
    created = "terraform"
  }
  
  retain_acked_messages      = false

  ack_deadline_seconds = 20

  enable_message_ordering    = false
}