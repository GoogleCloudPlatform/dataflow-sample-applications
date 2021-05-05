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

import os

# Model configuration used in training pipeline

MODEL_CONFIG = {
        # The number of timesteps used in the model.
        # This value must match the sequence length used in the java pipeline.
        'timesteps': 5,


        'features': [
                'timeseries_x-value-LAST',  # 'timeseries_x-value-FIRST',
                'timeseries_x-value-LAST_TIMESTAMP',
        ],
        'enable_timestamp_features': True,
        'time_features': ['MINUTE']
}

# Local configuration

PIPELINE_NAME = 'sin_wave_pipeline'

DEMO_DIR = '/tmp/demo'

LOCAL_PIPELINE_ROOT = f'{DEMO_DIR}/build/{PIPELINE_NAME}/pipeline_root'

LOCAL_DATA_ROOT = f'{DEMO_DIR}/{PIPELINE_NAME}/data_root'

SYNTHETIC_DATASET = {
        'local-bootstrap': f'{LOCAL_DATA_ROOT}/simple-data-5-step/tfx-data/data/',
        'local-raw': f'{LOCAL_DATA_ROOT}/simple-data-5-step/tfx-data/data/'
}

# KFP configuration

PROJECT_ID = ''

GCP_REGION = ''

GCS_BUCKET_NAME = ''

PIPELINE_ROOT = f'gs://{GCS_BUCKET_NAME}/{PIPELINE_NAME}/pipeline_root'

DATA_ROOT = f'gs://{GCS_BUCKET_NAME}/{PIPELINE_NAME}/data_root'

GCP_AI_PLATFORM_SERVING_ARGS = {
        'model_name': f'{PIPELINE_NAME}',
        'project_id': f'{PROJECT_ID}',
        'regions': [f'{GCP_REGION}'],
}

GCP_AI_PLATFORM_TRAINING_ARGS = {
        'project': f'{PROJECT_ID}',
        'region': f'{GCP_REGION}',
        'masterConfig': {
                'imageUri': 'gcr.io/' + f'{PROJECT_ID}' + f'/{PIPELINE_NAME}'
        },
}

GCP_DATAFLOW_ARGS = [
        '--runner=DataflowRunner',
        f'--project={PROJECT_ID}',
        f'--region={GCP_REGION}',
        '--temp_location=' + os.path.join('gs://', GCS_BUCKET_NAME, 'tmp')
]
