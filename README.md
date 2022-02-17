# Geotab Kafka Connect GCS Source Connector

What does this csv source connector do:
1. Designed and developed based on open-sourced kafka-connect framework.
2. Load csv files from Google Cloud Storage Buckets to Kafka with scalability and reliability.


## Introduction
   
This Kafka Connect GCS source connector provides the capability to read csv files from a GCS bucket
- Each new file pushed into the bucket will be monitored by Google Pub/Sub service
- Every record in the input file will be converted based on the user supplied schema. 
- The csv connectors can be deployed with simple create request
- The base source connector can be extended to other data format by customizing the `blobreader`

## Installation 
Please follow the steps for self-managed connectors on the Confluent site `https://docs.confluent.io/home/connect/self-managed/install.html`
Since this source connector is not integrated with confluent hub, it only supports manually installation.
Install Connector:
- use gradle to build a shadow jar for the csv source connector 
All the dependencies will be included in the shadow jar. `./gradlew shadow module-name`
- put the jar on your `kafka-connect-plugin` folder 
- Start/Restart the Kafka Connect workers with that configuration. Connect will discover all added connectors under the plugins.
The connector class is `com.geotab.dna.dataplatform.kafka.connect.source.gcs.CSVGcsSourceConnector` 
This connector is used to stream CSV files from a GCS bucket while converting the data based on the schema supplied in the configuration. 
This connector can be deployed on Kubernetes for auto-scaling and rebalance.
- template for source connector create request:
```
{
    "name": "source-connector-name",
    "config": {
    "connector.class": "com.geotab.dna.dataplatform.kafka.connect.source.gcs.CSVGcsSourceConnector",
    "topics": "csv-topic",
	"tasks.max":1,
    "schemaRegistryUrl": "http://schema-registry:8081",
    "schemaSubjectName": "csv-topic-value",
	"schemaSubjectVersion": "latest", 
	"gcsKeySource":"JSON",
	"gcsKeyfile":"{\n  \"type\": \"service_account\",\n  \"project_id\": \"test\",\n  \"private_key_id\": \"\"\n}\n", 
    "pubSubTopic" : "csv-connector-pubsub-topic",
	"pubSubSubscription" : "csv-connector-pubsub-topic-subscription",   
	"gcsProject": "test-project",
	"projectLocation": "eu",
	"cleanupStrategy": "ARCHIVE_TO_BUCKET",
    "gcsBucketName": "csv-connector-test", 
    "gcsErrorBucketName": "csv-connector-test-error",
    "gcsArchiveBucketName": "csv-connector-test-archive",
    "schemaRetriever" : "com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema.SchemaRegistrySchemaRetriever",
    "csv.skip.lines": 1,
    "csv.separator.char":",",
    "csv.ignore.quotations": true
    }
}
```

## Configs

| Configuration Name | Type | Default value | Description |
| ------------- | ------------- |------------- |------------- |
| gcsKeySource  | string |“”  |Determines whether the keyfile configuration is the path to the credentials JSON file or to the JSON itself. Available values are FILE and JSON. If left blank, Google Application Default Credentials will be used. |
| gcsKeyfile | string  |N/A|Can be either a string representation of the Google credentials file or the path to the Google credentials file itself |
| gcsProject| string| N/A| GCS Project of the source GCS bucket|
| projectLocation| string| N/A| The GCS, Pub/Sub resource zone| 
| gcsBucketName| string| N/A| Name of bucket where data needs to be pushed|
| autoCreateBucket| boolean| true|Determine whether we should create the bucket if it does not exist|
| gcsBucketName| string| N/A| Name of bucket where data needs to be pushed|
| gcsArchiveBucketName| string| N/A| Name of bucket where data needs to be archived. Required when the cleanup strategy is ARCHIVE_TO_BUCKET. This will be the bucket where we move blobs into once we complete processing|
| gcsErrorBucketName| string| N/A| Required when the cleanup strategy is ARCHIVE_TO_BUCKET. This will be the bucket where we move blobs with errors to once we complete processing|
| pubSubTopic| string| N/A| Name of the pub/sub topic that needs to be created in Pub/Sub| 
| pubSubTopicSubscription| string| N/A| Name of the subscription that needs to be used to pull data from Pub/Sub|
| cleanupStrategy| string| “DO_NOTHING”| This defines what needs to be done when the file is processed. Valid options are:DO_NOTHING,DELETE_WHEN_DONE,MOVE_TO_BUCKET|
| csv.skip.lines| int| 1| Number of lines to skip in the beginning of the file|
| csv.separator.char| string| ,| The character that separates each field in the form|
| csv.quote.char| string| N/A| The character that is used to quote a field| 
| csv.ignore.quotations| boolean| true|Sets the ignore quotations mode - if true, quotations are ignored|
 

## Important
This connector does not try to convert the csv records to a schema. User needs to provide the schema using avro format
 
## Contributors
Geotabbers and ex-Geotabbers:
[Mark](https://github.com/markma0215) /
[Lennon](https://github.com/LemonU) /
[Charan](https://github.com/charan478) /
[Ying](https://github.com/outerforce) /
[Wenyang](https://github.com/wyanglau) /
[Nate Qu](https://github.com/tsuxia) /
[Gerry](https://github.com/gerry-wu)



