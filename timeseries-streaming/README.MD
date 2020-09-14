# Overview

This repository comprises a reference pattern and accompanying sample code to process time-series data and derive insights using Machine Learning. In particular, a Keras model implementing an LSTM neural network for anomaly detection is provided. 

In addition, this repository provides a set of time-series transforms that simplify developing Apache Beam pipelines for processing ***streaming*** time-series data.

### Sample Version
0.3.1  
 - Options now passed through to all builders.
 - Refactor [PerfectRectangles](timeseries-java-applications/TimeSeriesPipeline/src/main/java/com/google/dataflow/sample/timeseriesflow/transforms/PerfectRectangles.java)
 - [Data Window Snapshot](timeseries-java-applications/TimeSeriesPipeline/src/main/java/com/google/dataflow/sample/timeseriesflow/transforms/GenerateMajorKeyWindowSnapshot.java)  
0.3.0 - Archived to Branch.

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

***Note - LAST / FIRST values***

Note, when exploring the examples, Type 1 aggregations are non-deterministic for FIRST or LAST value when they share the same timestamp. 

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

# General Patterns
 
## Gap Filling
Creating aggregations that rely purely on the data observed in the steam is straightforward. The Dataflow windowing functions, requiring only a couple of lines of code, can deal with the transitive and associative aggregations of finding the first, last, min, and max values within a time window. However, if the specific stream experiences periods of no data, the situation is not so straightforward. 

In the table below a D denotes a data point was seen for that time-series at that time interval. A '-' is the absence of data.
Window-00 is the first window between two time points since the start of the streaming pipeline. 

| Time Window |TS-1|TS-2|TS-3|TS-4|
| --- | --- | --- | --- | --- |
| Window-00 | D | D | - | D |
| Window-01 | D | - | - | - |
| Window-02 | D | D | D | - |
| Window-03 | D | - | D | - |
| Window-04 | D | D | D | - |

There are 4 cases to consider:
* TS-1: There is continuous data within the stream for all data windows. 
  * This is the base case and while it is simple to process, unless the windows are large, most streams do not have this characteristic.   
* TS-2: There are missing data points in the stream, but the key will continue to send data at some point.
  * How this is dealt with in stream mode, will depend on the use case;
    * If no data / action is required on the absence of data, then the basic window functions within Apache Beam can be used. 
    * If we do want data when there is no data entering the pipeline for a given window, then we can make use of the [Looping timers]([https://beam.apache.org/blog/2019/06/11/looping-timers.html) pattern to create an entry point for that window. 
* TS-3: There is no data at the bootstrap phase of the stream when compared with other time-series in the same data flows.
  * If a key has not been seen when the streaming pipeline has started, then the system does not know the key exists and the looping timer cannot start. However this problem can be solved by bootstrapping the pipeline with a flush of all needed keys on startup. The downside of this approach is that the data used for the flush must originate from the past, and this can cause duplicated data within the pipeline.
* TS-4: There is some data, but then no more data arrives for that key.
  * A simple example of this is an IOT device that is decommissioned and will never send data again. In this case if we have chosen to do gap-filling as described above, then we will need a way to signal a stop to the system.  
 
#### Gap filling in the sample
The experimental class [PerfectRectangles.java](timeseries-java-applications/TimeSeriesPipeline/src/main/java/com/google/dataflow/sample/timeseriesflow/transforms/PerfectRectangles.java)
makes use of looping timers to deal with the problem of trying to gap fill in the absence of data. In terms of the data to use to fill the gap, there are two modes, zero values and last known value.  
## Aggregations
* Type 1 - Transitive and associative computations
* Type 2 - Ordered aggregated input
  * Partial computations removed
  * Assumption that gaps always filled. 
## Merging of aggregations
* The Type 2 aggregations become merged with the Type 1 computations. 
 
# Data proto objects
There are three core objects used within the time-series pipelines which are defined 
in [TS.proto](timeseries-java-applications/TimeSeriesPipeline/src/main/proto/TS.proto)
* TSDatapoint
* TSAccum
* TSAccumSequence
The TS prefix stands for time-series and is used to easily distinguish the objects specific to this library. 
## Requirements
* The time-series objects used within the pipeline need to also exist outside of the pipeline.
* The user of the library should be free to provide any data type ( Integer, Float, Double, String ) and have the library be able to process the data ‘as-is’ without conversion. 
* The data object should hold an arbitrary list of data points and computations with arbitrary type along with metadata. 
### Data
The data proto holds a single datapoint object which is a oneof for different types of data. This is similar in concept to Tensroflow's tensor.proto however that proto makes use of a DataType to describe the type of a proto. In this data object the oneof enforces only a single type of data point.
### TSKey
There are two components to any time-series Key. The first is the Major key, which corresponds to the name of a Multivariate or Univariate time-series. For example, with a FSI example this would GBP-USD currency rates. For an IOT example this would be a sensor which records one or many data points. 
The Minor Key, when present is a single property of the Major keys values. 
For example, for GBP-USD it would be bid price or ask price. The minor key is important as it allows us to take a Multivariate time-series value and break it into different properties which may have different types and require different computations. 
### TSDataPoint
The TSDataPoint holds both the metadata and the data about the time-series data point. It contains the Data point, a timestamp and a Map to represent any metadata that would need to be added to the data point.
### TSAccum
The TSAccum contains the derived data from a univariate time-series. For example it will hold the First, Last, Sum, Min, Max values for a specific MajorKey-MinorKey. This object is the primary object holding data after computations and makes use of a Map to ensure any data point can be represented.
### TSAccumSequence
The Accum Sequence is an ordered collection of Accums, along with metadata about the window boundaries and associated metadata.
