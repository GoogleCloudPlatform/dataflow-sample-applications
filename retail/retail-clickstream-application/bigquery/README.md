# BigQuery

## Generate BigQuery Schema

Manually defining the schema for BigQuery with nested records and repeated records can be dificult so this repo contains an example file of JSON events that can be used to auto generate the schema.



Create BQ dataset and table and generate schema based on events

```
bq --location=US mk \
--dataset \
retail-data-demo:retail_dataset_generate
```

```
bq --location=US load --autodetect --source_format=NEWLINE_DELIMITED_JSON retail_dataset_generate.ecommerce_events_generate_schema ecommerce_events.json
```


```
bq show --format=prettyjson retail_dataset_generate.ecommerce_events_generate_schema > ecommerce_events_table_export.json
```

Use ctrl + j to convert multi-line to single line json