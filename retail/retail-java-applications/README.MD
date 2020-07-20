<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# Overview

This sample retail application, is intended to be used as an exemplar blueprint for similar applications. 

For this first version of the application the following items are shown:  
 
Processing and Analysis of:
* Clickstream data being sent by online systems to Cloud PubSub.
* Transaction data being sent by on-premise / saas systems to Cloud PubSub. 
* Stock data being sent by on-premise / saas systems to Cloud PubSub.

> Note: All data is sent as JSON, this is normal for clickstream data but not necessarily the norm for Transaction / Stock data. Different data formats (e.g xml)  will be shown in future versions.

## Requirements
* Requirement 1  

  The business has a requirement to process all of this incoming data and store it in our Data Warehouse, this allows for dashboards to be built using Looker. 
* Requirement 2

The business has a requirement to validate the data and apply corrections where possible to the data.

* Requirement 3

The business has a requirement to keep any bad data in a deadletter area for future correction. A running metric of this value should be available for monitoring and alerting. 

* Requirement 4

The business requires that the Transaction data for a store sale to be de-normalized to include lat lng positions of the store, this information is based upon a slow changing table in BigQuery which contains the store information driven off a store ID key. 

* Requirement 5

The business would like to compute the following analysis on the incoming data:

* Clickstream

Count the number of views per product in a given time period. 
Store the views per product in a low latency store for usage by the application to show 'number of people who viewed this product' in the website.

* Transactions & Stocks

The total number of sales per store and globally per item for a given period is to be computed alongside the total number of incoming stocks. This information is to be sent onto the inventory systems on a continuous basis. The inventory department will make use of this information for stock decisions. 
Implementation of the requirements using canonical patterns

# To run the pipeline locally you can make use of the smoke test 
gradlew :data-engineering-dept:pipelines:test --tests com.google.dataflow.sample.retail.pipeine.test.RetailDataProcessingPipelineSimpleSmokeTest --info --rerun-tasks

# Setup required for running the sample application on Dataflow service

Please note this application has many inputs and outputs.
All of these will need to be enabled in your project. 
Please consult 
*PubSub as both input and output.

### Create PubSub Topics and Subscriptions
####Create four topics:
* "Clickstream-inbound"
* "Transactions-inbound"
* "Inventory-inbound"
* "Inventory-outbound"

####Create three subscriptions
* "Clickstream-inbound-sub"
* "Transactions-inbound-sub"
* "Inventory-inbound-sub"

###Create BigQuery objects
####Create two dataset in BigQuery
* "Retail_Store"
* "Retail_Store_Aggregations"
####Create a location table
This table is used by the slow update side input pattern. 
The test data has only one store with id 1. 
```
CREATE TABLE
  Retail_Store.Store_Locations ( d INT64,
    city STRING,
    state STRING,
    zip INT64 );
INSERT INTO
  Retail_Store.Store_Locations
VALUES
  (1, 'a_city', 'a_state',00000); 
```
###Create a Bigtable instance
#### Create an instance
* Create a BigTable instance within your project
#### Create a table
* Create the "PageView5MinAggregates" table with Column family "pageViewAgg"

# Common Patterns
There are some common patterns required to achieve the business requirements, these are highlighted here along with a link to the code.
## Best practices for POJO usage,  AutoValue.
### Why needed?
Although in the long term the intention is for schemas to be the way that objects are represented within a pipeline, today there are still areas where POJO are needed, for example when dealing with KV objects or the state API. Hand building POJO objects requires the user to ensure correct override of equals / hashCode. If you don't, weird things might go wrong - it might work most of the time, and then suddenly you get some weird behavior or data loss.
### What is a good solution?
While AutoValue provides a lot of boilerplate for the user, it takes care of the important items. AutoValue is also heavily used within Beam code base itself, so familiarity with it is useful for Beam java pipeline builders.
### Example of usage:
[ClickstreamProcessing](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/transforms/clickstream/ClickstreamProcessing.java)
## Parsing JSON strings to Row's and POJO's
### Why needed?
One of the very common data types that are processed via Dataflow is JSON strings, for example clickstream information coming from browsers. These JSON strings will need to be processed into either Row objects or POJO for the duration of the pipeline processing.
### What is a good solution?
There are many ways to process this data, one of the very common methods with Java is to use Gson, which many Dataflow users make use of. Gson by default is relaxed in its processing of data, which while can be very useful can lead to situations where issues are not picked up quickly, requiring a lot more validation and user code to be built around the parsing process.
There is a utility class in Beam JsonToRow, while in 2.22.0 there are some limitations:
* Nulls must be strictly serialized in the json
* No easy way to build in deadletter pattern

The Apache Beam community is working on these features.
JsonToRow can be used for converting Json to Row and then onto Pojo objects. This makes use of the Convert utility class, which creates the flow:

Read -> JsonToRow -> Convert.fromRows()
### Example of usage
>Note: For v1.0 of the retail application we have created local copies of JsonToRow to incorporate some of the changes from Apache Beam master not available in 2.22.0. 

[ClickstreamProcessing.java](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/transforms/clickstream/ClickstreamProcessing.java)

[JSONUtils.java](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/utils/JSONUtils.java)
## Deadletter pattern

### Why needed?
In production systems, it is important to implement fail safes for problematic data points. If in-stream correction is not possible then at a minimum the value should be logged as an error, but more robustly the value should be sent intact to an external sync for later processing. An area where this is very common is when parsing data from one serialization format to another, for example the parsing of JSON objects. 

### What is a good solution?
The ideal situation is to use a multi-output transform to branch out the problematic elements into another PCollection for further processing. As this is a very common operation which you will make use of across many transforms within a pipeline, it is useful to create a generic transform that deals with problematic issues. 
Steps to create:
An error object, this can wrap common properties, including the original data.
A DeadLetter sink transform that has multiple options for the destination, designated by the business.  

### Example of usage
[JSONUtils.java](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/utils/JSONUtils.java)

[ErrorMsg.java](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/transforms/ErrorMsg.java)

[DeadLetterSink.java](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/transforms/DeadLetterSink.java)

## Data validation
### Why needed?
The data that is collected from external systems will not always be as clean as we would like, there could be errors in upstream systems, bugs etc ... The challenge of cleaning data is always an interesting one, in the traditional batch mode or stream, however in stream mode there are more considerations that we will need to take into account. We will not always be able to fix data as it comes into the system in real time. If it's not possible to fix, we should fall back to the standard deadletter pattern. However when the data is fixable we will want to branch the pipeline to send the 'bad' data to a correction transform(s). 
### What is a good solution?
Often there may be several different types of issues that a single message can suffer from, because of this we will need to think about the DAG that we construct to ensure that if we have elements that have multiple defects then the element can flow through all 'correct' transforms. 

* Option 1

For example imagine a element with these properties, which should never be null:
{foo: null,bar: null}

To fix this we can flow the element through to DoFn's, 

badElements.apply(fixFoo).apply(fixBar).

* Option 2

If we had errors which are mutually exclusive, for example foo is never null if bar is null and vice versa then we can write:

PCollectionTuple allBadElements = elements.apply(validateFooAndBar);
allBadElements.get(badFooElements).apply(fixFoo)
allBadElements.get(badBarElements).apply(fixBar)

Option 1 is recommended as its correct for both cases also, in option 2 if in the future the errors stop being mutually exclusive the pipeline would have to be reconstructed with a different shape. 

### Example of usage
[ValidateAndCorrectClickStreamEvents.java](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/transforms/clickstream/ValidateAndCorrectClickStreamEvents.java)

## External service calls considerations
### Why needed?
Because a pipeline can distribute work amongst many thousands of cores, its very easy to overwhelm calls to an external service endpoint if we make a single call per element flowing through the system. Especially if these are values pre any reducing functions. To accommodate this, its best to create batches of calls to an external system. There are several ways that its possible to achieve this :
### What is a good solution?
#### Use a GroupByKey transform or the utility GroupIntoBatches transform 
Allows for you to create a Iterable<element> which can be used to batch items into a single call within the DoFn.
* Pros
    1. No tech debt as its a Beam transform
* Cons
    1. Requires a shuffle
Requires magic number to determine the key space
#### Use State and Timer API to create our batches.
* Pros
    1. This has lots of control in the exact size of the batch that is created.
* Cons
    1. Requires a shuffle, which will be done on an arbitrary key space. Magic number time!
#### Use bundle start and bundle finish lifecycle elements to create our batches of data. This has the advantage of not requiring any shuffle steps in the processing.
* Pros
    1. No Shuffle needed
* Cons
    1. bundle sizes are non-deterministic, they are based on the runners implementations and often in stream mode will be very small in size.
 
>Note: In v1.0 of the sample application, option 3 is used. This may change in future version, or we may create another usecase which requires more fine grain control.

## Enrichment via slow updating side input
### Why needed?
A very common use case for streaming is to enrich data with information from what would be considered a dimension table in traditional data warehouse terminology. Essentially taking an element and denormalizing it by bringing in information contained  in a lookup table. For example if we have storeId and want the lat lng of that store looking it up in the table of our stores. In that example there is another characteristic , which is the table is not being updated very often, we don't add new stores every minute! So it makes sense to bring that table into the pipeline without having each element do an API call for its lookup. However we want to somehow update that table periodically to keep it fresh. 
### What is a good solution?
This pattern is well known and is covered by : https://beam.apache.org/documentation/patterns/side-inputs/
### Example of usage
SlowMovingStoreLocationDimension.java

## Using schemas for analytical calls
### Why needed?
Outside of the bread and butter work of a streaming pipeline;
* Data transport
* Data shaping
* Data verification
* Data enrichment

## We want to make use of our streaming pipeline to do real time analytics on the data, for this there are many paths available. However since schemas have been added to Beam, the way we do this analytics has now become much easier.
### What is a good solution?
By converting our objects to Row's we can start to make very clean java code, which abbreviates the 'DAG' building exercise we used to have to do before when needing to do analytics. We can then reference object properties as fields within the analytics statements that we are creating. As an example using the Group operation:
 Group.<ClickStreamEvent>byFieldNames("pageRef")
                .aggregateField("pageRef", Count.combineFn(), "count"))
### Example of usage
[CountViewsPerProduct.java](data-engineering-dept/business-logic/src/main/java/com/google/dataflow/sample/retail/businesslogic/core/transforms/clickstream/CountViewsPerProduct.java)