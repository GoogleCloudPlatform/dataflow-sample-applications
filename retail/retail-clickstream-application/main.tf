provider "google" {
  version = "3.48.0"

  project = var.project_id 
  region  = "us-central1"
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network"
  
}

resource "google_service_account" "data_pipeline_access" {
  project = var.project_id
  account_id = "retailpipeline"
  display_name = "My Tutorial Data pipeline access"
}

resource "google_project_iam_member" "dataflow_admin_role" {
  project = var.project_id
  role = "roles/dataflow.admin"
  member = "serviceAccount:${google_service_account.data_pipeline_access.email}"
}

resource "google_project_iam_member" "dataflow_worker_role" {
  project = var.project_id
  role = "roles/dataflow.worker"
  member = "serviceAccount:${google_service_account.data_pipeline_access.email}"
}

resource "google_project_iam_member" "bigquery_role" {
  project = var.project_id
  role = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.data_pipeline_access.email}"
}


resource "google_project_service" "run" {
  service = "run.googleapis.com"

  disable_on_destroy = false
}

resource "google_cloud_run_service" "pubsub_proxy" {
  name     = "pubsub-proxy"
  location = "us-central1"

  template {
    spec {
      containers {
        image = "gcr.io/${var.project_id }/pubsub-proxy"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.run]
}

resource "google_cloud_run_service_iam_member" "all_users" {
  service  = google_cloud_run_service.pubsub_proxy.name
  location = google_cloud_run_service.pubsub_proxy.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_pubsub_topic" "ps_topic" {
  name = "ecommerce-events"

  labels = {
    created = "terraform"
  }
}

resource "google_pubsub_subscription" "ps_subscription" {
  name  = "ecommerce-events-pull"
  topic = google_pubsub_topic.ps_topic.name

  labels = {
    created = "terraform"
  }
  
  retain_acked_messages      = false

  ack_deadline_seconds = 20


  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering    = false
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id                  = "retail_dataset"
  friendly_name               = "retail dataset"
  description                 = "This is a test description"
  location                    = "US"
  
  delete_contents_on_destroy = true

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "bq_table" {
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  table_id   = "ecommerce_events"

  time_partitioning {
    type = "DAY"
    field = "event_datetime"
  }

  labels = {
    env = "default"
  }

  schema = file("ecommerce_events_bq_schema.json")

}

resource "google_storage_bucket" "dataflow_gcs_bucket" {
    name = "${var.project_id}-ecommerce-events"
    force_destroy = true
}

resource "google_dataflow_job" "dataflow_stream" {
    name = "ecommerce-events-ps-to-bq-stream"
    template_gcs_path = "gs://dataflow-templates/latest/PubSub_Subscription_to_BigQuery"
    temp_gcs_location = "${google_storage_bucket.dataflow_gcs_bucket.url}/tmp_dir"

    parameters = {
      inputSubscription = google_pubsub_subscription.ps_subscription.id
      outputTableSpec   = "${google_bigquery_table.bq_table.project}:${google_bigquery_table.bq_table.dataset_id}.${google_bigquery_table.bq_table.table_id}"
    }

    transform_name_mapping = {
        name = "test_job"
        env = "dev"
    }

    on_delete = "cancel"
}

output "url_proxy" {
  value = google_cloud_run_service.pubsub_proxy.status[0].url
}