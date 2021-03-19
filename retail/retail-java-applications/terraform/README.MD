# Deploy using Terraform

Use Terraform to deploy the folllowing services defined in the `main.tf` file

- Pub/Sub

## Install Terraform

If you are using the [Google Cloud Shell](https://cloud.google.com/shell/docs/how-cloud-shell-works), Terraform is already installed.

Follow the instructions to [install the Terraform cli](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/gcp-get-started).

This repo has been tested on Terraform version `0.14.5` and the Google provider version `3.48.0`

## Edit Values of Variables If Desired
The values of variables can be found in terraform.tfvars as shown below. 

```
region                     = "us-central1"
topic_clickstream_inbound  = "Clickstream-inbound"
topic_transactions_inbound = "Transactions-inbound"
topic_inventory_inbound    = "Inventory-inbound"
topic_inventory_outbound   = "Inventory-outbound"
clickstream_inbound_sub    = "Clickstream-inbound-sub"
transactions_inbound_sub   = "Transactions-inbound-sub"
inventory_inbound_sub      = "Inventory-inbound-sub"
```

You can change these resources' names if there is a need to do so.

For example, if you want to change the topic name of clickstream inbound to "Clickstream-topic", just edit the name accordingly as shown below and save it.

```
topic_clickstream_inbound  = "Clickstream-topic"
```

However, the value "us-central1" must be a [valid region](https://cloud.google.com/compute/docs/regions-zones/viewing-regions-zones) in order to proceed.

Terraform will pick up these values during execution as described in the later section.

## Initialize Terraform

```
terraform init
```

You should see the following output

```
Initializing the backend...

Initializing provider plugins...
- Reusing previous version of hashicorp/google from the dependency lock file
- Installing hashicorp/google v3.48.0...
- Installed hashicorp/google v3.48.0 (signed by HashiCorp)

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.
```

## Create resources in Google Cloud

Run the plan command to see what resources will be created in your project. Please enter your project ID when prompted.
```
terraform plan
```
When prompted, please enter your project ID.

```
var.project_id
  Project ID in GCP

  Enter a value:
```
A list of resources planning to be created will be shown.

Run the apply command and point to your `.tfvars` file to deploy all the resources in your project.

```
terraform apply -var-file terraform.tfvars
```

In addition to what the plan command does, the apply command asks you to confirm your deployment action by entering `yes` to proceed:

```
Plan: 8 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```
If you are confident about the changes, you can add an additional flag "-auto-approve" to the apply command. This allows the apply command to proceed without typing `yes`.

```
terraform apply -auto-approve -var-file terraform.tfvars
```

## Terraform output

Once everything has successfully run you should see the following output:

```
google_project_service.pubsub: Creating...
.
.
.
google_pubsub_subscription.transactions_inbound_sub: Creation complete after 12s [id=projects/[your project ID]/subscriptions/Transactions-inbound-sub]
Apply complete! Resources: 8 added, 0 changed, 0 destroyed.
```
## Terraform Destroy

Use Terraform to destroy all resources
```
terraform destroy
```
## Automated Test For Modules
Please refer to [this document](test/README.md) to execute automated tests using Go and Terratest.

