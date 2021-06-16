# Disclaimer

These samples are for illustration of techniques that can be used with Dataflow. 
The samples are not a supported Google product. We welcome feedback, bug reports and code contributions, but cannot guarantee they will be
addressed.

# User guide
The draft user guide for 0.4.0 is available at [user guide](docs/Draft_Dataflow_streaming_time_series_sample_library_user_guide_v0.4.0.pdf)

# Overview

This repository comprises a reference pattern and accompanying sample code to process time-series data and derive insights using Machine Learning. In particular, a Keras model implementing an LSTM neural network for anomaly detection is provided. 

In addition, this repository provides a set of time-series transforms that simplify developing Apache Beam pipelines for processing ***streaming*** time-series data.


![Timeseries Metrics Image](images/timeseries_metrics.png?raw=true "Timeseries Metrics")


### Sample Version

0.4.0
 - Metrics creation framework refactor.
 - All options available as Pipeline Options
 - Draft User Guide 
 - Creation of VWAP Type 2 complex metric
 - Creation of ValueInBetween Type 2 complex metric
 - Introduction of Style standards for Metrics  
 - Example Pipeline updated
 - Remove use of internal data timestamp in favour of Beam Metadata Timestamp. 
   Add Verification to ensure error is thrown if Beam Metadata Timestamp != Data timestamp. 
   Allow over ride of hard failure if Timestamp != Data timestamp


0.3.3
 - Performance: Use Combiner rather then GBK for TSAccumSeq… 
 - Support GapFill behaviour Per Key 
 - Add log rtn metric
 - Wire the HB message through to output TSAccum 
 - Composite TSAccum Creation 

0.3.2
 - Upgrade of TFX version to 0.24.0
 - Use runtime values for timesteps and features to preprocessing_fn and input_fn
 - Allow inference to apply scaling to the input for comparision to the output from the model 

# Processing time-series data
Apache Beam has rich support for streaming data, including support for State and Timers API which enable sophisticated processing of time-series data. In order to effectively process streaming time-series data, practitioners often need to perform time-series data preprocessing. This can involve introducing computed data to fill 'gaps' in the source time-series data. 

For example, the following are example use cases where 'gaps' in time-series data needs to be filled. 

* IOT example:
    A device sends signals when something changes, and does not emit signals when there has been no change (e.g. to conserve battery power). However, the absence of data downstream does not necessarily mean that there is no information; it's just not been observed. Of course it could be the IOT device has lost function. In the absence of new data, either the last known value can be assumed, or a value can be inferred until some time-to-live is reached.
* Finance example:
    As the price of an asset changes, ‘ticks’ are produced. The Bid or Ask can update independently, but while there is no change, the last seen value can be assumed.

Another common requirement in processing time-series data is the need for
accessing data (e.g.  Last, First, Min, Max, Count values) from the previous
processing window when applying transforms to the current windows. For example,
we want to compute the rate of change between the First element in a window and 
the Last element in another window.

------

***NOTE*** 
The samples are currently experimental, specially classes like [PerfectRectangles](timeseries-java-applications/TimeSeriesPipeline/src/main/java/com/google/dataflow/sample/timeseriesflow/transforms/PerfectRectangles.java) use advanced techniques from the Apache Beam model. We expect to be hardening the samples over the next few iterations. 

Please do raise issues against the repo for any issues found when working with different datasets. 
 

------

# Quick start
There are two quickstart guides that can be followed that will spin up a demo of the samples.
## Apache Beam Java pipeline
The java pipeline reads data from a stream and generates metrics from that data. 
The simple-data used for illustration purposes can be seen in [SimpleDataStreamGenerator](timeseries-java-applications/SyntheticExamples/src/main/java/com/google/dataflow/sample/timeseriesflow/examples/simpledata/transforms/SimpleDataStreamGenerator.java). 

Use quick start in [README.MD](timeseries-java-applications/README.MD) for details.

##  The python samples have two components
### Apache Beam Python inference pipeline
The inference pipeline, consumes messages from a stream or file and uses the RunInference tfx-bsl transform to encode-decode the values. The decoded value compared against the original produces an absolute difference.   

### Tensorflow Extended pipeline used to build the model  

----

***Note - model quality***

The intent of this sample is to demonstrate the data engineering effort needed to support data generated from a streaming Beam pipeline and delivered to an LSTM autoencoder-decoder. It is not intended to demonstrate state or the art machine learning approaches to anomaly detection and the user is encouraged to replace the provided Keras model with their own.

The sample data is a simple repeating pattern, effectively making any train / eval / test void as the data is repeated across all samples.

----
Use quick start in [README.MD](timeseries-python-applications/README.MD) for details.

