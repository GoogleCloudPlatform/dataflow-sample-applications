#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
PIPELINE_ROOT = './build'
DATA_ROOT = '/tmp'

GCP_AI_PLATFORM_SERVING_ARGS = {
    'model_name': 'my_pipeline',
    'project_id': '',
    'regions': [""],
}

GCP_AI_PLATFORM_TRAINING_ARGS = {
    'project': '',
    'region': "",
    'masterConfig': {
        'imageUri': 'gcr.io/' + 'project' + '/pipeline-name'
    },
}

SYNTHETIC_DATASET = {
    'local-bootstrap': f'{DATA_ROOT}/simple-data-5-step/tfx-data/data/',
    'local-raw': f'{DATA_ROOT}/simple-data-5-step/tfx-data/data/'
}
