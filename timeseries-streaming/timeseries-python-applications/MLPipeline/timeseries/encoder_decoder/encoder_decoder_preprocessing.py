# Lint as: python2, python3
# Copyright 2020 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, Text, Any

import tensorflow as tf


def preprocessing_fn(inputs: Dict[Text, Any]) -> Dict[Text, Any]:
    """tf.transform's callback function for preprocessing inputs.

    Args:
      inputs: map from feature keys to raw not-yet-transformed features.

    Returns:
      Map from string feature key to transformed feature operations.
    """

    features = {}
    timesteps = 0
    """
    We use a workaround to get metadata to the transform using the name of the key.
    This can be removed once Transform supports having other parameters.
    """
    for key in inputs:
        if not key.startswith("METADATA_"):
            features[key] = inputs[key]
        if key.startswith("__CONFIG_TIMESTEPS"):
            timesteps = int(key.split("-")[1])

    # All float 32 types, note the TF.Example have been coerced
    train_float32_x_labels = []

    # All int types
    train_int64_x_labels = []

    for feature in features:
        values = features[feature].values
        if values.dtype == tf.float32:
            # TODO parametrize once supported
            if feature.endswith("-FIRST") or feature.endswith("-LAST"):
                train_float32_x_labels.append(feature)
        if values.dtype == tf.int64:
            train_int64_x_labels.append(feature)

    # The features are sorted in Lexicographical order, the inference will use this same sort.
    train_float32_x_labels.sort()
    train_int64_x_labels.sort()

    train_float32_x_values = []
    train_int64_x_values = []

    for k in train_float32_x_labels:
        train_float32_x_values.append(features[k].values)

    for k in train_int64_x_labels:
        train_int64_x_values.append(features[k].values)

    float32 = tf.reshape(
            tf.stack(train_float32_x_values, axis=1),
            [-1, timesteps, len(train_float32_x_values)])
    int64 = tf.reshape(
            tf.stack(train_int64_x_values, axis=1),
            [-1, timesteps, len(train_int64_x_values)])

    # AutoEncoder / AutoDecoder requires label == data
    outputs = {"Float32": float32, "LABEL": float32}

    return outputs
