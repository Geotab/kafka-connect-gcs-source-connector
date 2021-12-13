# Kafka-Connect-GCS-Source-Connector
Geotab Kafka Connect GCS source connector
 What is this csv source connector does:
1. Designed and developed based on open-sourced kafka-connect framework
2. Load csv files from Google Cloud Storage Buckets to Kafka with scalability and reliability


Benefits from open sourcing this:
1. contribute to the kafka opensource community
2. get feedback from the developers in the community and enhance the codebase
3. Geotab branding in Big Data Technologies

### Introduction
   
This Kafka Connect GCS source connector provides the capability to read csv files from a GCS bucket
- Each new file pushed into the bucket will be monitored by Google Pub/Sub service
- Every record in the input file will be converted based on the user supplied schema. 
- The csv connectors can be deployed with simple create request
- The base source connector can be extended to other data format by customizing the `blobreader`

### Installation 
Manually
- use gradle to build a shadow jar for the csv source connector and put it on your `kafka-connect-plugin` folder
All of the dependencies will be included in the shadow jar. `./gradlew shadow module-name`
- Restart the Kafka Connect worker.
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
### Important
This connector does not try to convert the csv records to a schema. User needs to provide the schema using avro format
 
 