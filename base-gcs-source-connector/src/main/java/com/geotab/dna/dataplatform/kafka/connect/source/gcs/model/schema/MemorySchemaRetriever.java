package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema;

import java.util.Map;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MemorySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(MemorySchemaRetriever.class);
  private static final int CACHE_SIZE = 1000;
  private Cache<String, Schema> schemaCache;

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public MemorySchemaRetriever() {
  }

  @Override
  public void configure(Map<String, String> properties) {
    schemaCache = new SynchronizedCache<>(new LRUCache<String, Schema>(CACHE_SIZE));
  }

  @Override
  public Schema retrieveSchema(String schemaSubject, String schemaVersion) {
    Schema schema = schemaCache.get(schemaSubject);
    if (schema != null) {
      return schema;
    }

    // By returning an empty schema the calling code will create a table without a schema.
    // When we receive our first message and try to add it, we'll hit the invalid schema case
    // and update the schema with the schema from the message
    return SchemaBuilder.struct().build();
  }

  @Override
  public void setLastSeenSchema(String topic, Schema schema) {
    logger.debug("Updating last seen schema to " + schema.toString());
    schemaCache.put(topic, schema);
  }
}
