# Click stream data - Data Layer to Pub/Sub

![Cloud Run Proxy](cloud_run_pubsub_proxy.png)

This repo provides an end to end example for streaming data from a webstore to BigQuery. It contains the following components that can be deployed all at once using Terraform or serve as indvidual examples.

- Cloud Run service that can be set-up as a custom tag in Google Tag Manager.
- Pub/Sub topic to consume the data
- Pub/Sub subscription to pull the data from the topic
- Dataflow streaming job using a Pub/Sub to BigQuery template
- BigQuery events table to store the data
- BigQuery SQL query to analyse the events

The data structure is based on the [Data Layer Ecommerce](https://developers.google.com/tag-manager/ecommerce-ga4) format recommended for Google Tag Manager.

## Git clone repo

```
git clone https://github.com/GoogleCloudPlatform/dataflow-sample-applications.git
cd dataflow-sample-applications/retail/retail-clickstream-application
```

## Set-up Cloud Environment

### Initilize your account and project

```shell
gcloud init
```

### Set Google Cloud Project

```
export GOOGLE_CLOUD_PROJECT=my-project-id
gcloud config set project $GOOGLE_CLOUD_PROJECT
```

### Enable Google Cloud APIs

```
gcloud services enable compute.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com
```

### Set compute zone

```
gcloud config set compute/zone us-central1-f
```

# Build container

```
export RUN_SOURCE_DIR=cloud-run-pubsub-proxy

gcloud builds submit $RUN_SOURCE_DIR --tag gcr.io/$GOOGLE_CLOUD_PROJECT/pubsub-proxy
```

### List containers

Check that the container was succesfully created

```
gcloud container images list
```

You should see the following output:

```
NAME
gcr.io/my-project-id/pubsub-proxy
Only listing images in gcr.io/my-project-id. Use --repository to list images in other repositories.
```


## Deploy using Terraform

Use Terraform to deploy the folllowing services defined in the `main.tf` file

- Cloud Run
- Pub/Sub
- Google Cloud Storage
- Dataflow
- BigQuery

### Install Terraform

Follow the instructions to [install the Terraform cli](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/gcp-get-started).

This repo has been tested on Terraform version `0.13.5` and the Google provider version `3.48.0`

### Update Project ID in terraform.tfvars

Rename terraform.tfvars.example file to terraform.tfvars and update the default project ID in terraform.tfvars file to match your project ID.

You can also use this command to replace the default project ID in the terraform.tfvars file.

```
sed "-i" "" "-e" 's/default-project-id/'"$GOOGLE_CLOUD_PROJECT"'/g' terraform.tfvars
```

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

### Create resources in Google Cloud

Run the plan cmd to see what resources will be greated in your project.

```
terraform plan
```

Run the apply cmd and point to your `.tfvars` file to deploy all the resources in your project.

```
terraform apply -var-file terraform.tfvars
```

This will show you a plan of everything that will be created and then the following notification where you should enter `yes` to proceed:

```
Plan: 17 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: 
```

### Terraform output

Once everything has succesfully run you should see the following output:

```
google_compute_network.vpc_network: Creating...
.
.
.
google_compute_network.vpc_network: Creation complete after 44s [id=projects/default-project-id/global/networks/terraform-network]

Apply complete! Resources: 14 added, 0 changed, 0 destroyed.

Outputs:

cloud_run_proxy_url = https://pubsub-proxy-my-service-id-uc.a.run.app
```

## Simulate sending ecommerce events to Cloud Run Pub/Sub proxy using curl

Use the url_output value from the Terraform output to simulate sending ecommerce events to the Cloud Run Pub/Sub proxy.

```
export CLOUD_RUN_PROXY=https://pubsub-proxy-my-service-id-uc.a.run.app
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

Run the following queries to check the data in BigQuery

```
bq query --use_legacy_sql=false \
'SELECT 
  event_datetime, event, user_id
FROM `retail_dataset.ecommerce_events`
LIMIT 10'
```

```
bq query --use_legacy_sql=false \
'SELECT 
  event, 
  COUNT(DISTINCT ecommerce.purchase.transaction_id ) as transactions,
  SUM(ecommerce.purchase.value) as revenue
FROM `retail_dataset.ecommerce_events`
WHERE event = "purchase"
GROUP BY event'
```

### Terraform Destroy

Use Terraform to destroy all resources

```
terraform destroy
```