package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema;


import io.confluent.connect.avro.AvroData;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the Confluent Schema Registry to fetch the latest schema for a given topic.
 */
public class SchemaRegistrySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistrySchemaRetriever.class);

  private SchemaRegistryClient schemaRegistryClient;
  private AvroData avroData;
  private ProtobufData protobufData;

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public SchemaRegistrySchemaRetriever() {
  }

  // For testing purposes only
  SchemaRegistrySchemaRetriever(SchemaRegistryClient schemaRegistryClient, AvroData avroData) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.avroData = avroData;
  }

  @Override
  public void configure(Map<String, String> properties) {
    SchemaRegistrySchemaRetrieverConfig config =
        new SchemaRegistrySchemaRetrieverConfig(properties);
    Map<String, ?> schemaRegistryClientProperties =
        config.originalsWithPrefix(config.SCHEMA_REGISTRY_CLIENT_PREFIX);
    schemaRegistryClient = new CachedSchemaRegistryClient(
        config.getString(config.LOCATION_CONFIG),
        0,
        schemaRegistryClientProperties
    );
    avroData = new AvroData(config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG));
    protobufData = new ProtobufData(config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG));
  }

  @Override
  public Schema retrieveSchema(String schemaSubject, String schemaVersion) {
    try {
      SchemaMetadata latestSchemaMetadata = retrieveSchemaMetadata(schemaSubject, schemaVersion);
      switch (latestSchemaMetadata.getSchemaType()) {
        case "AVRO":
          org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(latestSchemaMetadata.getSchema());
          return avroData.toConnectSchema(avroSchema);
        case "PROTOBUF":
          ProtobufSchema timestampSchema = new ProtobufSchema(com.google.protobuf.Timestamp.getDescriptor());

          ProtobufSchema protobufSchema = new ProtobufSchema(latestSchemaMetadata.getSchema(), latestSchemaMetadata.getReferences(),
              Collections.singletonMap(timestampSchema.name(), timestampSchema.canonicalString()), latestSchemaMetadata.getVersion(), schemaSubject);

          return protobufData.toConnectSchema(protobufSchema);
        default:
          return null;
      }

    } catch (IOException | RestClientException exception) {
      throw new ConnectException(String.format("could not fetch schema for subject=%s, version=%s", schemaSubject, schemaVersion), exception);
    }
  }

  @Override
  public void setLastSeenSchema(String topic, Schema schema) {
  }

  public org.apache.avro.Schema retrieveAvroSchema(String schemaSubject, String schemaVersion) {
    try {
      SchemaMetadata latestSchemaMetadata = retrieveSchemaMetadata(schemaSubject, schemaVersion);
      if (latestSchemaMetadata.getSchemaType().equals("AVRO")) {
        return new org.apache.avro.Schema.Parser().parse(latestSchemaMetadata.getSchema());
      } else {
        return null;
      }
    } catch (IOException | RestClientException exception) {
      throw new ConnectException(String.format("could not fetch schema for subject=%s, version=%s", schemaSubject, schemaVersion), exception);
    }
  }

  private SchemaMetadata retrieveSchemaMetadata(String schemaSubject, String schemaVersion) throws RestClientException, IOException {
    logger.debug("Retrieving schema information for schema subject {} with version ID {}", schemaSubject, schemaVersion);
    int schemaVersionID = schemaVersion != null && schemaVersion.matches("\\d+") ? Integer.parseInt(schemaVersion) : -1;
    return schemaVersionID == -1 ? schemaRegistryClient.getLatestSchemaMetadata(schemaSubject)
        : schemaRegistryClient.getSchemaMetadata(schemaSubject, Integer.parseInt(schemaVersion));
  }
}
