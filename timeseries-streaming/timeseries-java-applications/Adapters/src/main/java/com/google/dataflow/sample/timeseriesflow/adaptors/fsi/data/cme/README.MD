# Quickstart
This is an introduction to the Data Adapters to convert [CME SmartStream JSON](https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON) market data feed
 into [TSDataPoints](https://github.com/GoogleCloudPlatform/dataflow-sample-applications/blob/master/timeseries-streaming/timeseries-java-applications/TimeSeriesPipeline/src/main/proto/TS.proto#L58)

#### [SSCLTOBJsonTransform](CMEAdapter.java)

[Smart Stream Cloud Link - Top of Book](https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON#CMESmartStreamonGCPJSON-BookMessage) Transform

Converts [TopOfBookEvent](TopOfBook.java) JSON events into
 TSDataPoints

1. Convert JSON from Pub/Sub to Top of Book Row Schema
2. Extract Ask & Bid and other key elements - [symbolAskBid](Data.java)
3. Convert Extracted Ask Bid Row Schema to TSDataPoints with following attributes
   * tradingStatus
   * periodCode
   * productCode
   * productType
   * productGroup
   * askLevelOrderCnt
   * askLevelPrice
   * askLevelQty
   * bidLevelOrderCnt
   * bidLevelPrice
   * bidLevelQty
4. All TSDataPoints are stamped with ```askLevelLastUpdateTime``` Timestamp

   <ul>
      <li>Input Timestamps are assumed to be in Central Time Zone, so done the required conversions 
      in the class</li>
   </ul>
  
   When specifying ```DeadLetterSink.BIGQUERY```, the ```BigQueryIO.write()``` is used with
    following options
   <ul>
     <li>WRITE_APPEND</li>
     <li>CREATE_IF_NEEDED</li>
   </ul>
 

<u>Example Usage:</u>
```
    // Convert Top of Book JSON records to TSDataPoint
    PCollection<TSDataPoint> tobTSDataPoint =
        pipeline
            .apply(Create.of(expected).withCoder(StringUtf8Coder.of()))
            .apply(
                SSCLTOBJsonTransform.newBuilder()
                    .setDeadLetterSinkType(DeadLetterSink.LOG)
                    .setBigQueryDeadLetterSinkProject(null)
                    .setBigQueryDeadLetterSinkTable(null)
                    .build());
```

#### [SSCLTRDJsonTransform](CMEAdapter.java)

[Smart Stream Cloud Link - Trade Information (Time and
 Sales)](https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON#CMESmartStreamonGCPJSON-TradeMessage) Transform
 
Converts [TradeInfoEvent](TradeInfo.java) JSON events into
 TSDataPoints



1. Convert JSON from Pub/Sub to Trade Info Row Schema
2. Extract Trade Information key elements - [symbolTradeInfo](Data.java)
3. Convert Extracted Trade Info Row Schema to TSDataPoints with following attributes
   * tradePrice
   * tradeQty
   * tradeOrderCount
   * tradeUpdateAction
   * periodCode
   * productCode
   * productType
   * productGroup
4. All TSDataPoints are stamped with ```lastUpdateTime``` Timestamp

  <ul>
      <li>Input Timestamps are assumed to be in Central Time Zone, so done the required conversions 
      in the class</li>
   </ul>
  
   When specifying ```DeadLetterSink.BIGQUERY```, the ```BigQueryIO.write()``` is used with
    following options
   <ul>
     <li>WRITE_APPEND</li>
     <li>CREATE_IF_NEEDED</li>
   </ul>
  
<u>Example Usage:</u>
```
    // Convert Trade Info JSON records to TSDataPoint
    PCollection<TSDataPoint> trdTSDataPoint =
        pipeline
            .apply(Create.of(expected).withCoder(StringUtf8Coder.of()))
            .apply(
                SSCLTRDJsonTransform.newBuilder()
                    .setDeadLetterSinkType(DeadLetterSink.LOG)
                    .setBigQueryDeadLetterSinkProject(null)
                    .setBigQueryDeadLetterSinkTable(null)
                    .build());
```