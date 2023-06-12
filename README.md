# Taxirides Pipeline Example

This repository contains an example of streaming Dataflow pipeline to create sessions based on input payload attributes using State and Timers APIs

## Input

Public Google Cloud Pub/Sub topic: `projects/pubsub-public-data/topics/taxirides-realtime`

That topic contains messages from the NYC Taxi Ride dataset; here is a sample of the data contained in a message:

```
{
"ride_id": "328bec4b-0126-42d4-9381-cb1dbf0e2432",
"point_idx": 305,
"latitude": 40.776270000000004,
"longitude": -73.99111,
"timestamp": "2020-03-27T21:32:51.48098-04:00",
"meter_reading": 9.403651,
"meter_increment": 0.030831642,
"ride_status": "enroute",
"passenger_count": 1
}
```

The message also contain metadata that is useful for streaming pipelines. In this case, the messages contain an attribute of name `ts`, which contains the same timestamp as the field of name timestamp in the data. Remember that Pub/Sub treats the data as just a string of bytes (in topics with no schema), so it does not know anything about the data itself. The metadata fields are normally used to publish messages with specific ids and/or timestamps.

To inspect the messages from this topic, you can create a subscription, and then pull some messages.

To create a subscription, use the `gcloud` cli utility (installed by default in the Google Cloud Shell):

```
export TOPIC=projects/pubsub-public-data/topics/taxirides-realtime
gcloud pubsub subscriptions create taxis --topic $TOPIC
```

To pull messages:

```
gcloud pubsub subscriptions pull taxis --limit 3
```

or if you have jq (for pretty printing of JSON)

```
gcloud pubsub subscriptions pull taxis --limit 3 | grep " {" | cut -f 2 -d ' ' | jq
```

## Execute Job

To execute the job with `DirectRunner`

```
python -m taxirides_pipeline \
--taxirides_subscription ${INPUT_SUBSCRIPTION} \
--bigquery_stat_events_table ${BIGQUERY_STAT_EVENTS_TABLE} \
--raw_events_flag ${RAW_EVENTS_FLAG} \
--bigquery_raw_events_table ${BIGQUERY_RAW_EVENTS_TABLE} \
--runner DirectRunner \
--job_name ${JOB_NAME}
```

To execute the job with `DataflowRunner`

```
python -m taxirides_pipeline \
--taxirides_subscription ${INPUT_SUBSCRIPTION} \
--bigquery_stat_events_table ${BIGQUERY_STAT_EVENTS_TABLE} \
--raw_events_flag ${RAW_EVENTS_FLAG} \
--bigquery_raw_events_table ${BIGQUERY_RAW_EVENTS_TABLE} \
--runner DataflowRunner \
--project ${PROJECT} \
--region ${REGION} \
--temp_location ${TEMP_LOCATION} \
--subnetwork ${SUBNETWORK} \
--service_account=${SERVICE_ACCOUNT} \
--no_use_public_ips \
--job_name ${JOB_NAME} \
--setup_file ./setup.py
```

_Make sure to configure the appropriate environment variables before running the job_