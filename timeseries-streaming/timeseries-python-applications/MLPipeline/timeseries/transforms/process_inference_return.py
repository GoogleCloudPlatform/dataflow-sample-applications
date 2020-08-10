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
from __future__ import absolute_import

from datetime import datetime
from typing import Dict, Text, Any

import tensorflow as tf

import apache_beam as beam
from tensorflow_serving.apis import prediction_log_pb2


class ProcessReturn(beam.DoFn):
    """
    We need to match the input to the output to compare the example to the encoded-decoded value.
    The transform component preprocessing_fn creates lexical order of the features in scope for the model.
    This function mimics the preprocessing_fn structure.
    """

    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    def __init__(self, *unused_args, **unused_kwargs):
        beam.DoFn.__init__(self)

    def process(
            self,
            element: prediction_log_pb2.PredictionLog,
            *unused_args,
            **unused_kwargs):
        request_input = element.predict_log.request.inputs['examples']
        request_output = element.predict_log.response.outputs['output_0']

        # Create the list of features based on the TF.Example used as input
        example = tf.train.Example.FromString(request_input.string_val[0])
        feature_labels = []
        features = example.features.feature

        if len(request_input.string_val) > 1:
            raise Exception("Only support single input string.")

        span_start_timestamp = datetime.fromtimestamp(
                features['METADATA_SPAN_START_TS'].int64_list.value[0] / 1000)
        span_end_timestamp = datetime.fromtimestamp(
                features['METADATA_SPAN_END_TS'].int64_list.value[0] / 1000)

        for f in features:
            # Remove metadata which is not used by the model
            if not (f.startswith("METADATA_") or f.startswith("__CONFIG_")):
                # Current model only uses the Float32 values
                if len(features[f].float_list.value) > 0:
                    if f.endswith("-FIRST") or f.endswith("-LAST"):
                        feature_labels.append(f)
        # Sort the values by lexical order
        feature_labels.sort()
        # Connect the result to the input and output as tuple
        results = tf.io.parse_tensor(
                request_output.SerializeToString(), tf.float32).numpy()

        result = {
                'span_start_timestamp': span_start_timestamp,
                'span_end_timestamp': span_end_timestamp
        }
        timestep_counter = 0
        for batches in results:
            for timestep in batches:
                feature_pos = 0
                for value in timestep:
                    # Find which label this item matches.
                    label = (feature_labels[feature_pos])
                    # Find the input value from the float list for this sample
                    input_value = features[label].float_list.value[
                            timestep_counter]
                    result[label] = {
                            'input_value': input_value,
                            'output_value': value,
                            # Outliers will effect the head of their array, so we need to keep the array to show in
                            # the outlier detection.
                            'raw_data_array': str(
                                    features[label].float_list.value),
                            'timestep': timestep_counter
                    }
                    feature_pos += 1
                # Only yield last value
                timestep_counter += 1
        # Output the last value only, which is the last in the pos list
        yield result


class CheckAnomalous(beam.DoFn):
    """
    Naive threshold based entirely on % difference cutoff value.
    """

    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    def __init__(self, threshold: int = 5, *unused_args, **unused_kwargs):
        beam.DoFn.__init__(self)
        self.threshold = threshold

    def process(self, element: Dict[Text, Any], *unused_args, **unused_kwargs):

        for k in element.keys():
            if k.endswith('-LAST') or k.endswith('-FIRST'):
                span_start_timestamp = element['span_start_timestamp']
                span_end_timestamp = element['span_end_timestamp']
                input_value = element[k]['input_value']
                output_value = element[k]['output_value']
                raw_data = element[k]['raw_data_array']
                diff = abs(input_value - output_value)
                if diff > self.threshold:
                    yield f'Outlier detected for {k} at {span_start_timestamp} - {span_end_timestamp} Difference was {diff} for value input {input_value} ' \
                          f'output {output_value} with raw data {raw_data}'
