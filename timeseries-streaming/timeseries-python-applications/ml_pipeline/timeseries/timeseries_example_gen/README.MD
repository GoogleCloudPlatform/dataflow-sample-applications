### TimeSeriesExampleGen ###
This components consumes tf.example and tf.sequence_example files which carry timestamp information as part of their 
features. This Metadata is used to create Spans of data where the Train / Eval and Test splits are based on 
non-overlapping discrete temporal indexes. 


The library can work with both TF.Example and TF.SequenceExample proto's. Each proto is expected to have two
metadata features:

#first_timestamp :#
This is the timestamp of the earliest data point in the sample.
The feature name which corresponds to first_timestamp is passed in as a parameter to the component,
`first_timestamp_name`

#last_timestamp :#
This is the timestamp of the last data point in the sample, if this value does not exist or is null
then it will be assumed that this element has only one data point at time equal to the first timestamp
The feature name which corresponds to last_timestamp is passed in as a parameter to the component,
`last_timestamp_name`

Examples:

Given a collection where first_timestamp is set and last_timestamp is null and first_timestamp's are:

[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]

A Train 6 Validate 2 Test 2 ratio split would result in a Span with:

Train,     [0, 1000, 2000, 3000, 4000, 5000]

Validate,  [6000, 7000]

Test,      [8000, 9000]

Timeline:

The timeline has a minimum time of min(first_timestamp) and max(last_timestamp) if last_timestamp's are null or max(last_timestamp) if they are not null. 
In this version there can only be one span, which is the whole of the timeline. 

The order in which the Timeline will be consumed for the span will be based on the ordinal positions:

[Training , Validation, Testing]

Configuration:

The primary configuration will be based on the ratio provided for Test / Validate and Train. 

Known issues:

If providing a Span configuration where there is overlapping data from the timeline for example:

Timeline = [0, 1000, 2000, 3000, 4000, 5000]

Span-0
	train-split	[0,1000,2000]
	eval-split 	[3000]

Span-1 
	train-split	[0,1000,2000,3000]
	eval-split 	[4000]

Then components like statistics_gen will not be accurate 



