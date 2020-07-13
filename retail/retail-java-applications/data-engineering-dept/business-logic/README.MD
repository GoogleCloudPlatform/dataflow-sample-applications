<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# Clickstream Processing

All business transformations for the processing and analysis of clickstream data.

# Main Process Clickstream:
 * Read Click Stream Topic
 * Parse Messages to Beam SCHEMAS
 * Branch 1:
   * Write RAW JSON String Clickstream for storage
 * Branch 2:
   * Clean the data
   * Write Cleaned Data to BigQuery
 * Branch 2.1:
   * Filter out events of type ERROR
   * Count Page Views per product in 5 sec windows
   * Export page view aggregates to BigTable
   * Export page view aggregates to BigQuery 
   
# Main Transactions Processing

All business transformations for the processing and analysis of transaction data.

Process Transaction:
 * Read transaction Stream Topic
 * Branch 1:
   * Write RAW JSON String Transaction for storage
 * Branch 2:
   * Parse Messages to Beam SCHEMAS
   * Convert to transaction object
   * Enrich the data
   * Write enriched Data to BigQuery

All business transformations for the processing and analysis of stock data.

# Main Stock Processing
 * Read stock Stream Topic
 * Branch 1:
   * Write RAW JSON String stock for storage
 * Branch 2:
   * Parse Messages to Beam SCHEMAS
   * Convert to Stock Object
   * Write Data to BigQuery
  
# Utility Transaction
 * Count sales per item per store within a fixed window
 * Count sales per item within a fixed window
  
# Utility Stock
 * Count incoming stock per item per store within a fixed window
 * Count incoming stock per item within a fixed window
 