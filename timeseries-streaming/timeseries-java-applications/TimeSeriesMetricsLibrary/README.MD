# Quickstart

In this section we will explain the metrics available and how to develop new ones, some of the metrics are relevant as technical indicators in finance only, but could still be used as time series features for models in other domains.
The current metrics in scope are (Feel free to add new ones following the below steps!):

Metric | Description | Parameters | Status
--- | --- | --- | ---
Simple Moving Average | Measures trend using simple average | None | DONE
Exponential Moving Average | Measures trend using exponential average | Alpha: Smoothing factor | DONE
Weighted Moving Average | Measures trend using weighted average | Weight: Smoothing factor | **TODO**
Standard Deviation | Measures volatility/risk/dispersion | None | DONE
Bollinger Bands | Measures price volatility in bands | Alpha, averageComputationMethod, devFactor | DONE
Relative strength index | Measures historical and current pricing trends | None | DONE


## Creating new metrics from a stream
1. Fork in Github, clone the repository and create a new local branch ```git clone https://github.com/GoogleCloudPlatform/dataflow-sample-applications;git checkout -b <MY_METRIC>```
2. Add the name of the new metric/s in the Protobuf object increasing the unique identifier number accordingly [TSFSITechKeys.proto](src/main/proto/TSFSITechKeys.proto), e.g., ```<MY_METRIC>=18;``` 
3. Create your test case in [TSMetricsTests](src/test/java/com/google/dataflow/sample/timeseriesflow/metrics/TSMetricsTests.java), you could reuse one of the existing test cases similar to your new metric
4. Copy and refactor the following classes from an existing metric, e.g. Moving Average:
    - Metric [Autovalue builder](https://github.com/google/auto/blob/master/value/userguide/builders.md) object builder, e.g., [MA.java](src/main/java/com/google/dataflow/sample/timeseriesflow/metrics/MA.java)
    - Metric [Autovalue builder](https://github.com/google/auto/blob/master/value/userguide/builders.md) Accumulator object builder, e.g., [AccumMABuilder.java](src/main/java/com/google/dataflow/sample/timeseriesflow/metrics/AccumMABuilder.java)
    - DoFn class, e.g., [ComputeSimpleMovingAverage.java](src/main/java/com/google/dataflow/sample/timeseriesflow/metrics/ComputeSimpleMovingAverageDoFn.java)
5. Add/modify the necessary getters and setters in your metric object builders
6. Modify your DoFn accordingly and create the mathematical formula as new method in the class [StatisticalFormulas.class](src/main/java/com/google/dataflow/sample/timeseriesflow/metrics/utils/StatisticalFormulas.java), so others could reuse these formulas outside your new metric.
7. Run the [TSMetricsTests](src/test/java/com/google/dataflow/sample/timeseriesflow/metrics/TSMetricsTests.java) class to validate the results then build with Gradle
8. Finally commit your changes, push to your Github fork and create a PR to review and merge in the upstream repository