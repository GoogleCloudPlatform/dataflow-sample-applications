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
This folder contains a dashboard with several charts that will be useful for monitoring your application.

You can deploy the dashboard to your Cloud Monitoring workspace by invoking the Cloud Monitoring API using ```curl``` or the ```gcloud``` command-line tool.  

## Before you begin
Ensure that you already have a Cloud Monitoring Workspace, and have the host project ID of your Workspace ready.

You also need to have the ```Monitoring Dashboard Configuration Editor``` role. If you intend to use the ```gcloud``` command-line tool, you also need to have the ```Service Usage Consumer``` role.

## Setup ##
1. Create an environment variable to hold the ID of your Cloud Monitoring Workspace: 
```
PROJECT_ID=a-gcp-project-12345
```

2. Authenticate to Cloud SDK:
```
gcloud auth login
```

3. Optionally, you can avoid having to specify your project ID with each command by setting it as the default by using Cloud SDK:
```
gcloud config set project ${PROJECT_ID}
```

## Using ```curl``` ##
1. Create an authorization token and capture it in an environment variable:
```
ACCESS_TOKEN=`gcloud auth print-access-token`
```
You have to periodically refresh the access token. If commands that worked suddenly report that you're unauthenticated, re-issue this command.

2. To verify that you got an access token, echo the ```ACCESS_TOKEN``` variable:
```
echo ${ACCESS_TOKEN}
ya29.ImW8Bz56I04cN4qj6LF....
```

3. To create the dashboard, send a POST request to the Cloud Monitoring endpoint:
```
curl -d @retail_pipeline.json -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' -X POST https://monitoring.googleapis.com/v1/projects/${PROJECT_ID}/dashboards
```

## Using ```gcloud``` ##
1. To create the dashboard, use the ```gcloud monitoring dashboards create command```:
```
gcloud monitoring dashboards create --config-from-file=retail_pipeline.json
```