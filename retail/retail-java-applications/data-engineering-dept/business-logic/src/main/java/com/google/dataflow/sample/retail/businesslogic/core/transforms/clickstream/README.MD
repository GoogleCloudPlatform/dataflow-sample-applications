#Clickstream

All business transformations for the processing and analysis of clickstream data.

Process Clickstream:
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