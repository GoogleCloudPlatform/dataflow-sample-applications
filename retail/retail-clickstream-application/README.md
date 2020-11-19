# Click stream data - Data Layer to Pub/Sub

![Cloud Run Proxy](cloud-run-proxy.png)

This repo contains example code to show how to send data from a ecommerce website to Pub/Sub using Google Tag Manager, Cloud Run and Pub/Sub.

The data structure uses the [Data Layer Ecommerce](https://developers.google.com/tag-manager/ecommerce-ga4) format recommended for Google Tag Manager

## Git clone repo

```
git clone https://github.com/GoogleCloudPlatform/dataflow-sample-applications.git
cd dataflow-sample-applications/retail/clickstream
```

## Set-up Cloud Environment

### Initilize your account and project

```shell
gcloud init
```

### Enable Cloud Cloud APIs

```
gcloud services enable run.googleapis.com cloudbuild.googleapis.com pubsub.googleapis.com
```

### Set compute zone

```
gcloud config set compute/zone us-central1-f
```

### Set Google Cloud Project

```
export GOOGLE_CLOUD_PROJECT=my-project-id
```


# Build container

```
export LOCAL_SOURCE_DIR=cloud-run-pubsub-proxy

gcloud builds submit $LOCAL_SOURCE_DIR --tag gcr.io/$GOOGLE_CLOUD_PROJECT/pubsub-proxy
```

If you receive permission error below give the service account access to GCS

```
ERROR: (gcloud.builds.submit) INVALID_ARGUMENT: could not resolve storage source: googleapi: Error 403: 12345678999@cloudbuild.gserviceaccount.com does not have storage.objects.get access to the Google Cloud Storage object., forbidden
```

### List containers

Check that the container was succesfully created

```
gcloud container images list
```

## Deploy using Terraform

Use Terraform to deploy the folllowing services

- Cloud Run
- Pub/Sub
- Google Cloud Storage
- Dataflow
- BigQuery

### Install Terraform

Follow the instructions to [install the Terraform cli](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/gcp-get-started)

### Initialize Terraform

```
terraform init
```

You should see the following output

```
Initializing the backend...

Initializing provider plugins...
- Checking for available provider plugins...
- Downloading plugin for provider "google" (hashicorp/google) 3.48.0...

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.
```

### Create resoureces in Google Cloud

~Note: Since Terraform 0.12 and above you no longer need to run `terraform plan` first.~

```
terraform apply -var-file terraform.tfvars
```

This will show you a plan of everything that will be created and then the following notification where you should enter `yes` to proceed:

```
Plan: 14 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: 
```

### Terraform output

Once everything has succesfully run you should see the following output:

```
google_compute_network.vpc_network: Creating...
google_pubsub_topic.ps_topic: Creating...
google_service_account.data_pipeline_access: Creating...
google_project_service.run: Creating...
google_bigquery_dataset.bq_dataset: Creating...
google_storage_bucket.dataflow_gcs_bucket: Creating...
google_bigquery_dataset.bq_dataset: Creation complete after 1s [id=projects/retail-data-demo/datasets/retail_dataset]
google_bigquery_table.bq_table: Creating...
google_storage_bucket.dataflow_gcs_bucket: Creation complete after 1s [id=retail-data-demo-ecommerce-events]
google_service_account.data_pipeline_access: Creation complete after 2s [id=projects/retail-data-demo/serviceAccounts/retailpipeline@retail-data-demo.iam.gserviceaccount.com]
google_bigquery_table.bq_table: Creation complete after 2s [id=projects/retail-data-demo/datasets/retail_dataset/tables/ecommerce_events]
google_project_iam_member.dataflow_admin_role: Creating...
google_project_iam_member.dataflow_worker_role: Creating...
google_project_iam_member.bigquery_role: Creating...
google_pubsub_topic.ps_topic: Creation complete after 4s [id=projects/retail-data-demo/topics/ecommerce-events]
google_pubsub_subscription.ps_subscription: Creating...
google_project_service.run: Creation complete after 4s [id=retail-data-demo/run.googleapis.com]
google_cloud_run_service.pubsub_proxy: Creating...
google_pubsub_subscription.ps_subscription: Creation complete after 4s [id=projects/retail-data-demo/subscriptions/ecommerce-events-pull]
google_dataflow_job.dataflow_stream: Creating...
google_compute_network.vpc_network: Still creating... [10s elapsed]
google_dataflow_job.dataflow_stream: Creation complete after 4s [id=2020-11-19_06_54_47-10288703401036332425]
google_project_iam_member.bigquery_role: Creation complete after 9s [id=retail-data-demo/roles/bigquery.dataEditor/serviceaccount:retailpipeline@retail-data-demo.iam.gserviceaccount.com]
google_project_iam_member.dataflow_admin_role: Still creating... [10s elapsed]
google_project_iam_member.dataflow_worker_role: Still creating... [10s elapsed]
google_project_iam_member.dataflow_admin_role: Creation complete after 10s [id=retail-data-demo/roles/dataflow.admin/serviceaccount:retailpipeline@retail-data-demo.iam.gserviceaccount.com]
google_project_iam_member.dataflow_worker_role: Creation complete after 10s [id=retail-data-demo/roles/dataflow.worker/serviceaccount:retailpipeline@retail-data-demo.iam.gserviceaccount.com]
google_cloud_run_service.pubsub_proxy: Still creating... [10s elapsed]
google_compute_network.vpc_network: Still creating... [20s elapsed]
google_cloud_run_service.pubsub_proxy: Still creating... [20s elapsed]
google_cloud_run_service.pubsub_proxy: Creation complete after 22s [id=locations/us-central1/namespaces/retail-data-demo/services/pubsub-proxy]
google_cloud_run_service_iam_member.all_users: Creating...
google_compute_network.vpc_network: Still creating... [30s elapsed]
google_cloud_run_service_iam_member.all_users: Creation complete after 8s [id=v1/projects/retail-data-demo/locations/us-central1/services/pubsub-proxy/roles/run.invoker/allusers]
google_compute_network.vpc_network: Still creating... [40s elapsed]
google_compute_network.vpc_network: Creation complete after 44s [id=projects/retail-data-demo/global/networks/terraform-network]

Apply complete! Resources: 14 added, 0 changed, 0 destroyed.

Outputs:

url_proxy = https://pubsub-proxy-zzheer6emq-uc.a.run.app
```
## Simulate sending ecommerce events to Cloud Run Pub/Sub proxy using curl

Use the url_output value from the Terraform output to simulate sending ecommerce events to the Cloud Run Pub/Sub proxy.

```
export CLOUD_RUN_PROXY=https://pubsub-proxy-zzheer6emq-uc.a.run.app
```

```
curl -vX POST ${CLOUD_RUN_PROXY}/json -d @datalayer/view_item.json \
--header "Content-Type: application/json"
```

```
curl -vX POST ${CLOUD_RUN_PROXY}/json -d @datalayer/add_to_cart.json \
--header "Content-Type: application/json"
```

```
curl -vX POST ${CLOUD_RUN_PROXY}/json -d @datalayer/purchase.json \
--header "Content-Type: application/json"
```

### Query Data in BigQuery

Run the following Query to check the data in BigQuery

```sql
SELECT 
  event, 
  COUNT(DISTINCT ecommerce.purchase.transaction_id ) as transactions,
  SUM(ecommerce.purchase.value) as revenue
FROM `retail-data-demo.retail_dataset.ecommerce_events`
WHERE event = 'purchase'
GROUP BY event
```

## Create Pub/Sub Topic and Subscription

### Create Pub/Sub topic

```
gcloud pubsub topics create ecommerce-events
```

### Create Pub/Sub subscription

```
gcloud pubsub subscriptions create ecommerce-events-pull --topic=ecommerce-events
```

## Create a Cloud Run proxy called datalayer-pubsub

As you can send data directly to Pub/Sub from a public facing website you will need to create a Cloud Run proxy which will be deployed as tag on your website using Google Tag Manager.

Modify the file `cloud-run-pubsub-proxy/app.js` on line 26 to inclue the Pub/Sub topic name your created in the previous step



## Deploy container to Cloud Run

```
gcloud run deploy datalayer-pubsub \
  --image gcr.io/$GOOGLE_CLOUD_PROJECT/datalayer-pubsub \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

You should see the following output that has your Cloud Run app's URL

```
Deploying container to Cloud Run service [datalayer-pubsub] in project [retail-data-demo] region [us-central1]
✓ Deploying... Done.
  ✓ Creating Revision...  
  ✓ Routing traffic...
  ✓ Setting IAM Policy... 
Done.
Service [datalayer-pubsub] revision [datalayer-pubsub-00002-kuw] has been deployed and is serving 100 percent of traffic at https://datalayer-pubsub-123abcd-uc.a.run.app
```


## Pull data from Pub/Sub subscription

You may need to run this multiple times to see the result.

```shell
gcloud pubsub subscriptions pull retail-clickstream-sub --auto-ack --limit=10
```

## Create BigQuery Table

```
CREATE TABLE retail_website.raw_clickstream ( event STRING, user_id INT64 )
```

### Check dataflow erros

```
SELECT errorMessage, payloadString, COUNT(*) 
FROM `retail-data-demo.retail_website.raw_clickstream_error_records`
GROUP BY errorMessage, payloadString
ORDER BY 3 DESC
LIMIT 1000
```

### Send example Pub/Sub data

```
{"date":"2020-11-16", "datetime":"2020-11-16 20:59:59", "event":"pageview", "user_id":123456}
{"date":"2020-11-16", "datetime":"2020-11-16 20:59:59", "event":"view_item", "user_id":123456, "ecommerce": {"items":[{"item_name": "T-Shirt","item_id": "67890"}]}}
```

[
  {
    "name": "date",
    "type": "DATE",
    "mode": "NULLABLE",
    "description": "Date of Event"
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
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "User ID"
  },
    {
        "name": "ecommerce",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": {
            "name": "items",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "item_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "item_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        }
    }
]

```
bq --location=US load \
--autodetect \
--source_format=NEWLINE_DELIMITED_JSON \
retail_dataset.test_events \
events.json
```

```
{ "date":"2020-11-16",
  "datetime":"2020-11-16 20:59:59",
  "event": "view_item",
  "user_id": 1234567,
  "ecommerce": {
    "items": [{
      "item_name": "Donut Friday Scented T-Shirt",
      "item_id": "67890"
    }]
  }
}
```

```
{ "date":"2020-11-16",
  "datetime":"2020-11-16 20:59:59",
  "event": "view_item",
  "user_id": 1234567,
  "ecommerce": {
    "items": [{
      "item_name": "Donut Friday Scented T-Shirt",
      "item_id": "67890",
      "price": "33.75",
      "item_brand": "Google",
      "item_category": "Apparel",
      "item_category_2": "Mens",
      "item_category_3": "Shirts",
      "item_category_4": "Tshirts",
      "item_variant": "Black",
      "item_list_name": "Search Results",  
      "item_list_id": "SR123",  
      "index": 1,  
      "quantity": "1"
    }]
  }
}
```

## Clean up

### Delete Container Image

```
gcloud container images delete gcr.io/$GOOGLE_CLOUD_PROJECT/datalayer-pubsub
```

### Delete Cloud Run service

```
gcloud run services delete datalayer-pubsub \
  --platform managed \
  --region us-central1
```