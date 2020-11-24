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
import math
from typing import Dict, Text, Any, Tuple
import tensorflow as tf

MINUTE = 60.0
HOUR = 3600.0
DAY = 86400.0
MONTH = 30.42 * 86400.0
YEAR = 365.2425 * 86400.0


def create_feature_list_from_list(features: [Text],
                                  config: Dict[Text, Any]) -> [Text]:
    """
    Given a list of features, create the features defined in config to be added to the model.
    The values are returned in lexical order, to allow for post-inference processing
    """
    feature_list = config['features']

    # Discard features not required in the model
    model_features = [k for k in features if k in feature_list]

    # All timestamps have suffix of _Timestamp

    if config['enable_timestamp_features']:
        time_features = []
        for k in feature_list:
            if k.endswith('_TIMESTAMP'):
                time_feature = create_time_coordinate_features_names(
                    k, config=config)
                time_features.extend(time_feature)
                # Remove the original value from the list
                model_features.remove(k)
        model_features.extend(time_features)

    # Sort the features to ensure we can match results to inputs based on lexicographical order.
    model_features.sort()
    return model_features


def create_feature_list_from_dict(
        features: Dict[Text, Any], config: Dict[Text, Any]) -> Dict[Text, Any]:
    """
    Produce a feature value dict, create the features defined in config to be added to the model.
    """
    feature_list = config['features']

    # Discard features not required in the model
    model_features = {k: features[k] for k in features if k in feature_list}

    if len(model_features) != len(feature_list):
        raise ValueError(f'Input is missing features input has {model_features.keys()} '
                         f'required is {feature_list}')

    # Convert to dense
    model_features = {k: tf.sparse.to_dense(features[k]) for k in model_features}

    if config['enable_timestamp_features']:
        timestamp_feature_list = [
            k for k in model_features if k.endswith('_TIMESTAMP')
        ]
        model_features = {
            k: v
            for k,
                v in model_features.items() if not k.endswith('_TIMESTAMP')
        }

        timestamp_features = [
            create_time_coordinate_features((k, tf.sparse.to_dense(features[k])), config)
            for k in timestamp_feature_list if k.endswith('_TIMESTAMP')
        ]

        for k in timestamp_features:
            model_features.update(k)

    return model_features


def create_time_coordinate_features_names(
        feature: Text, config: Dict[Text, Any]) -> [Text]:
    """
    For any timeseries feature, return sin and cos features to the
    resolution dictated via 'time_features'.
    """
    if not feature.endswith("_TIMESTAMP"):
        raise ValueError(f'Feature {feature} does not end with TIMESTAMP')
    requested_time_features = config['time_features']
    if not set(requested_time_features).issubset(
            ['MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR']):
        raise ValueError(
            f'Only MINUTE HOUR DAY YEAR are supported not {requested_time_features}'
        )

    feature_name = removesuffix(feature, '_TIMESTAMP')

    sin = [f'{feature_name}-SIN-{k}-TIMESTAMP' for k in config['time_features']]
    cos = [f'{feature_name}-COS-{k}-TIMESTAMP' for k in config['time_features']]
    cos_sin_feature = sin + cos

    return cos_sin_feature


def create_time_coordinate_features(
        features: Tuple[Text, Any], config: Dict[Text, Any]):
    """
    For any timeseries feature, return sin and cos features to the
    resolution dictated via 'time_features'.
    """
    cos_sin_feature = create_time_coordinate_features_names(
        feature=features[0], config=config)

    return {
        k: tf.cast(create_time_coordinate(k, features[1]), tf.float32)
        for k in cos_sin_feature
    }


def create_time_coordinate(feature: str, timestamp_int64: Any):
    """
    Returns value in float64 for best precision, post function would normally drop down to float32

    """
    # We remove ms from the timestamp else will overflow
    timestamp = tf.cast(timestamp_int64 / 1000, tf.float64)
    if feature.endswith('-SIN-MINUTE-TIMESTAMP'):
        return tf.math.sin(timestamp * (2.0 * math.pi / MINUTE))

    if feature.endswith('-COS-MINUTE-TIMESTAMP'):
        return tf.math.cos(timestamp * (2.0 * math.pi / MINUTE))

    if feature.endswith('-SIN-HOUR-TIMESTAMP'):
        return tf.math.sin(timestamp * (2.0 * math.pi / HOUR))

    if feature.endswith('-COS-HOUR-TIMESTAMP'):
        return tf.math.cos(timestamp * (2.0 * math.pi / HOUR))

    if feature.endswith('-SIN-DAY-TIMESTAMP'):
        return tf.math.sin(timestamp * (2.0 * math.pi / DAY))

    if feature.endswith('-COS-DAY-TIMESTAMP'):
        return tf.math.cos(timestamp * (2.0 * math.pi / DAY))

    if feature.endswith('-SIN-MONTH-TIMESTAMP'):
        return tf.math.sin(timestamp * (2.0 * math.pi / MONTH))

    if feature.endswith('-COS-MONTH-TIMESTAMP'):
        return tf.math.cos(timestamp * (2.0 * math.pi / MONTH))

    if feature.endswith('-SIN-YEAR-TIMESTAMP'):
        return tf.math.sin(timestamp * (2.0 * math.pi / YEAR))

    if feature.endswith('-COS-YEAR-TIMESTAMP'):
        return tf.math.cos(timestamp * (2.0 * math.pi / YEAR))


def removesuffix(self: str, suffix: str) -> str:
    """
    TODO remove with 3.9 python
    """
    # suffix='' should not call self[:-0].
    if suffix and self.endswith(suffix):
        return self[:-len(suffix)]
    return self[:]


def print_feature_pos(config: Dict[Text, Any]):
    """
    Utility to show the position of features from the config file.
    Note if a feature is missing from the source data this will
    not be == to the features used tfx pipeline.
    """
    features = create_feature_list_from_list(config=config, features=config['features'])
    for i in range(len(features)):
        print(f'Feature pos {i} is {features[i]}')
