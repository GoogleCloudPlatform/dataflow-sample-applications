# Automated Test For Terraform Modules

Use Go and Terratest to test the following modules.
- Pub/Sub

## Install Terraform and Go

If you are using the [Google Cloud Shell](https://cloud.google.com/shell/docs/how-cloud-shell-works), both Terraform and Go are installed.

Follow the instructions to install [Terraform cli](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/gcp-get-started) and [Go](https://golang.org/doc/install).

This repo has been tested on Terraform version `0.14.5`, Go version `1.16` and Google provider version `3.48.0`

## Terraform Folder Structure


The Terraform folder structure can be found below. 

Note that the `terraform` folder is inside `dataflow-sample-applications/retail/retail-java-applications` folder.

```
terraform
├── examples
│   └── pubsub
│       ├── main.tf
│       ├── terraform.tfvars
│       └── variables.tf
├── modules
│   └── pubsub
│       ├── main.tf
│       └── variables.tf
├── test
│   ├── pubsub
│   │   ├── go.mod
│   │   ├── go.sum
│   │   └── pubsub_test.go
│   └── README.md
├── README.MD
├── terraform.tfvars
├── main.tf
└── variables.tf
```

The `examples` folder contains Terraform scripts that serve as living documentation about how to use the module. They are also used by automated tests for execution.

The `modules` folder contains the scripts that form the basis of the module.

The `test` folder contains the test code.

The `terraform` folder contains Terraform scripts which deploy required resources for `retail-java-application` using corresponding Terraform modules.

## Resources Being Tested

### Pub/Sub

In this test, four topics and three subscriptions will be created in Pub/Sub with prefixes described below followed by a randomised string that consists of 5 lowercase letters. 

For example, the topic name could be `Clickstream-inbound-kpocn` while its corresponding subscription name could be `Clickstream-inbound-sub-wtutp` in the automated test.

This prevents potential failures for subsequent executions when [creating and deleting topics and subscriptions](https://cloud.google.com/pubsub/docs/admin#deleting_a_topic) using the same names rapidly.

**Topics**
- Clickstream-inbound
- Transactions-inbound
- Inventory-inbound
- Inventory-outbound

**Subscriptions**
- Clickstream-inbound-sub
- Transactions-inbound-sub
- Inventory-inbound-sub

Once these resources are created successfully, the test will publish messages using the topics and receive messages using the subscriptions. 

Messages sent and received must match in order to pass the test. 

Finally, all resources will be destroyed in order to keep a clean test environment.


## Export Your Project ID

Please replace `[your project ID]` with your own project ID in the following command and execute it:

`export GOOGLE_PROJECT=[your project ID]`

The test will pick up the project ID from environment variables automatically.

## Execute Automated Test


1. Make sure you are in the test folder: `dataflow-sample-applications/retail/retail-java-applications/terraform/test/pubsub`
2. Execute the test using `go test -v`


You will see the following output.

```
=== RUN   TestPubsub
Cleaning up conflicting resources if any...
TestPubsub 2021-03-31T16:56:39+08:00 retry.go:91: terraform [init -upgrade=false]
TestPubsub 2021-03-31T16:56:39+08:00 logger.go:66: Running command terraform with args [init -upgrade=false]
TestPubsub 2021-03-31T16:56:39+08:00 logger.go:66: Initializing modules...
.
.
.
TestPubsub 2021-03-31T16:57:28+08:00 logger.go:66: 
TestPubsub 2021-03-31T16:57:28+08:00 logger.go:66: Destroy complete! Resources: 8 destroyed.
--- PASS: TestPubsub (52.51s)
    --- PASS: TestPubsub/group (0.00s)
        --- PASS: TestPubsub/group/ClickstreamInbound (3.47s)
        --- PASS: TestPubsub/group/InventoryInbound (4.04s)
        --- PASS: TestPubsub/group/TransactionsInbound (4.52s)
PASS
ok      dataflow-sample-applications/retail/retail-java-applications/terraform/test     52.521s
```
Congratulations! Your test passed successfully!


## When Terraform Destroy Fails
Please follow the instructions described in each component to delete the resources manually.
### Pub/Sub
1. In Google Cloud Platform Console, go to `Navigation Menu --> Pub/Sub --> Subscriptions`.
2. Select all existing subscriptions and click `DELETE` button at the top
3. Click `Topics` on the left panel
4. Select all existing topics and click `DELETE` button at the top

