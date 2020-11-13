provider "google" {
  version = "3.47.0"

  project = "retail-data-demo" 
  region  = "us-central1"
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network"
}

resource "google_project_service" "run" {
  service = "run.googleapis.com"
}

resource "google_cloud_run_service" "hello-service" {
  name     = "hello-service"
  location = "us-central1"

  template {
    spec {
      containers {
        image = "gcr.io/cloudrun/hello"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.run]
}

resource "google_cloud_run_service" "pubsub-proxy" {
  name     = "pubsub-proxy"
  location = "us-central1"

  template {
    spec {
      containers {
        image = "gcr.io/retail-data-demo/datalayer-pubsub"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.run]
}

resource "google_cloud_run_service_iam_member" "allUsers" {
  service  = google_cloud_run_service.pubsub-proxy.name
  location = google_cloud_run_service.pubsub-proxy.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_pubsub_topic" "retail-clickstream" {
  name = "retail-clickstream"

  labels = {
    created = "terraform"
  }
}

resource "google_pubsub_subscription" "retail-clickstream-sub" {
  name  = "retail-clickstream-sub"
  topic = google_pubsub_topic.retail-clickstream.name

  labels = {
    created = "terraform"
  }

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = false

  ack_deadline_seconds = 20

  expiration_policy {
    ttl = "300000.5s"
  }
  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering    = false
}

resource "google_bigquery_dataset" "retail_dataset" {
  dataset_id                  = "retail_dataset"
  friendly_name               = "retail dataset"
  description                 = "This is a test description"
  location                    = "US"
  
  delete_contents_on_destroy = true

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "raw_clickstream" {
  dataset_id = google_bigquery_dataset.retail_dataset.dataset_id
  table_id   = "raw_clickstream"

  time_partitioning {
    type = "DAY"
    field = "datetime"
  }

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {
    "name": "date",
    "type": "DATE",
    "mode": "NULLABLE",
    "description": "Datetime of Event"
  },{
    "name": "datetime",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime of Event"
  },{
    "name": "event",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Event Name"
  },
  {
    "name": "user_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "User ID"
  },
  {
        "name": "ecommerce",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
          {
            "fields": [
              {
                "mode": "NULLABLE",
                "name": "item_id",
                "type": "INTEGER"
              },
              {
                "mode": "NULLABLE",
                "name": "item_name",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "price",
                "type": "FLOAT64"
              },
              {
                "mode": "NULLABLE",
                "name": "item_brand",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "item_category",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "item_category_2",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "item_category_3",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "item_category_4",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "item_variant",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "item_list_name",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "item_list_id",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "index",
                "type": "INT64"
              },
              {
                "mode": "NULLABLE",
                "name": "quantity",
                "type": "INT64"
              }
            ],
            "mode": "REPEATED",
            "name": "items",
            "type": "RECORD"
          }
        ]
      }
]
EOF

}

resource "google_storage_bucket" "retail_clickstream_bucket" {
    name = "retail-clickstream-test-bucket"
    force_destroy = true
}

resource "google_dataflow_job" "pubsub_stream" {
    name = "tf-clickstream-dataflow-job"
    template_gcs_path = "gs://dataflow-templates/latest/PubSub_Subscription_to_BigQuery"
    temp_gcs_location = "${google_storage_bucket.retail_clickstream_bucket.url}/tmp_dir"

    parameters = {
      inputSubscription = google_pubsub_subscription.retail-clickstream-sub.id
      outputTableSpec    = "${google_bigquery_table.raw_clickstream.project}:${google_bigquery_table.raw_clickstream.dataset_id}.${google_bigquery_table.raw_clickstream.table_id}"
    }

    transform_name_mapping = {
        name = "test_job"
        env = "dev"
    }

    on_delete = "cancel"
}

output "url" {
  value = google_cloud_run_service.hello-service.status[0].url
}

output "url_proxy" {
  value = google_cloud_run_service.pubsub-proxy.status[0].url
}