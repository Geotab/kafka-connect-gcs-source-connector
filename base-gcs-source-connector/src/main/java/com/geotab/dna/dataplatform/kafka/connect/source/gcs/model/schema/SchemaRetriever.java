package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema;


import java.util.Map;
import org.apache.kafka.connect.data.Schema;


public interface SchemaRetriever {

  public void configure(Map<String, String> properties);

  public Schema retrieveSchema(String schemaSubject, String schemaVersion);

  public void setLastSeenSchema(String topic, Schema schema);
}
