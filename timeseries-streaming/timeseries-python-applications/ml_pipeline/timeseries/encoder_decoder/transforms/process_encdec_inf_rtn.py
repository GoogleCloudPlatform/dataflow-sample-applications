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
import tensorflow_transform as tft
from tensorflow_serving.apis import prediction_log_pb2

import apache_beam as beam
from apache_beam.utils.windowed_value import WindowedValue
from ml_pipeline.timeseries.utils import timeseries_transform_utils


class ProcessReturn(beam.DoFn):
    """
    We need to match the input to the output to compare the example to the encoded-decoded value.
    The transform component preprocessing_fn creates lexical order of the features in scope for the model.
    This function mimics the preprocessing_fn structure.
    """

    def __init__(self, config: Dict[Text, Any], batching_size: int = 1000):
        beam.DoFn.__init__(self)
        self.tf_transform_graph_dir = config['tf_transform_graph_dir']
        self.model_config = config['model_config']
        self.batching_size = batching_size

    def setup(self):
        self.transform_output = tft.TFTransformOutput(self.tf_transform_graph_dir)
        self.tft_layer = self.transform_output.transform_features_layer()

    def start_bundle(self):
        self.batch: [WindowedValue] = []

    def finish_bundle(self):
        for prediction in self.process_result(self.batch):
            yield prediction

    def process(
            self,
            element: prediction_log_pb2.PredictionLog,
            window=beam.DoFn.WindowParam,
            timestamp=beam.DoFn.TimestampParam):
        if len(element.predict_log.request.inputs['examples'].string_val) > 1:
            raise Exception("Only support single input string.")

        if len(self.batch) > self.batching_size:
            for k in self.process_result(self.batch):
                yield k
            self.batch.clear()
        else:
            self.batch.append(WindowedValue(element, timestamp, [window]))

    def process_result(self, element: [WindowedValue]):
        """
        A input example has shape : [timesteps, all_features] all_features is
         not always == to features used in model.
        An output example has shape : [timesteps, model_features]

        In order to compare these we need to match the (timestep, feature) from
        (timestep,all_features) to (timestep, model_features)

        There are also Metadata fields which provide context

        """
        element_value = [k.value for k in element]
        processed_inputs = []
        request_inputs = []
        request_outputs = []

        for k in element_value:
            request_inputs.append(
                    k.predict_log.request.inputs['examples'].string_val[0])
            request_outputs.append(k.predict_log.response.outputs['output_0'])

        # The output of tf.io.parse_example is a set of feature tensors which
        # have shape for non Metadata of [batch,
        # timestep]

        batched_example = tf.io.parse_example(
                request_inputs, self.transform_output.raw_feature_spec())

        # The tft layer gives us two labels 'FLOAT32' and 'LABEL' which have
        # shape [batch, timestep, model_features]

        inputs = self.tft_layer(batched_example)

        print(request_outputs)

        # Determine which of the features was used in the model
        feature_labels = timeseries_transform_utils.create_feature_list_from_list(
                features=batched_example.keys(), config=self.model_config)

        # The outer loop gives us the batch label which has
        # shape [timestep, model_features]
        # For the metadata the shape is [timestep, 1]

        metadata_span_start_timestamp = tf.sparse.to_dense(
                batched_example['METADATA_SPAN_START_TS']).numpy()
        metadata_span_end_timestamp = tf.sparse.to_dense(
                batched_example['METADATA_SPAN_END_TS']).numpy()

        batch_pos = 0
        for batch_input in inputs['LABEL'].numpy():
            # Get the Metadata from the original request
            span_start_timestamp = datetime.fromtimestamp(
                    metadata_span_start_timestamp[batch_pos][0] / 1000)
            span_end_timestamp = datetime.fromtimestamp(
                    metadata_span_end_timestamp[batch_pos][0] / 1000)
            # Add the metadata to the result
            result = {
                    'span_start_timestamp': span_start_timestamp,
                    'span_end_timestamp': span_end_timestamp
            }

            # In this loop we need to compare the last timestep
            # [timestep , model_feature] for the input and the output.

            # Get the output that matches this input
            results = tf.io.parse_tensor(
                    request_outputs[batch_pos].SerializeToString(),
                    tf.float32).numpy()[0]

            # The last time step is the last value in the input batch,
            # ordinal pos starts from 0
            last_timestep_pos = len(batch_input) - 1

            # From the input batch get the last time step
            last_timestep_input = batch_input[last_timestep_pos]

            # Get the last timestep from the results
            last_timestep_output = results[last_timestep_pos]

            feature_results = {}
            for model_feature_pos in range(len(last_timestep_output)):
                label = (feature_labels[model_feature_pos])

                # The num of features should == number of results
                if len(feature_labels) != len(last_timestep_input):
                    raise ValueError(f'Features list {feature_labels} in config is '
                                     f'len {len(feature_labels)} which '
                                     f'does not match output length '
                                     f'{len(last_timestep_output)} '
                                     f' This normally is a result of using a configuration '
                                     f'file that does not match '
                                     f'tf_transform dir / saved model dir.')

                # The num of features should == number of results
                if len(last_timestep_output) != len(last_timestep_input):
                    raise ValueError(f'Input len {len(last_timestep_input)} does not '
                                     f'match output length {len(last_timestep_output)} '
                                     f' This normally is a result of mis matched tf_transform dir and saved model dir.')

                feature_results[label] = {
                        'input_value': last_timestep_input[model_feature_pos],
                        'output_value': last_timestep_output[model_feature_pos]
                }
                if not str(label).endswith('-TIMESTAMP'):
                    feature_results[label].update({
                            # Outliers will effect the head of their array, so we need to keep the array
                            # to show in the outlier detection.
                            'raw_data_array': str(
                                    tf.sparse.to_dense(
                                            batched_example[label]).numpy()
                                    [batch_pos])
                    })

            result.update({'feature_results': feature_results})
            processed_inputs.append(result)

            batch_pos += 1

        # Add back windows
        windowed_value = []
        for input_pos in range(len(processed_inputs) - 1):
            windowed_value.append(
                    element[input_pos].with_value(processed_inputs[input_pos]))
        return windowed_value


class CheckAnomalous(beam.DoFn):
    """
    Naive threshold based entirely on % difference cutoff value.
    """

    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    def __init__(self, threshold: float = 0.05):
        beam.DoFn.__init__(self)
        self.threshold = threshold

    def process(self, element: Dict[Text, Any], *unused_args, **unused_kwargs):
        result = {
                'span_start_timestamp': element['span_start_timestamp'],
                'span_end_timestamp': element['span_end_timestamp']
        }

        for key, value in element['feature_results'].items():
            input_value = value['input_value']
            output_value = value['output_value']
            diff = abs(input_value - output_value)
            value.update({'diff': diff})
            if not key.endswith('-TIMESTAMP'):
                value.update({'anomaly': diff > self.threshold})
            result.update({key: value})
        yield result
