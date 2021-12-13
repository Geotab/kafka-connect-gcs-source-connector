package com.geotab.dna.dataplatform.kafka.connect.source.gcs.reader;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema.SchemaRetriever;
import com.github.javafaker.Bool;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;

public interface BlobReader {
  void configure(AbstractConfig config);

  default void postConfigure(Object... objects) {
  }

  default Map<String, Object> getMetrics() {
    return new HashMap<>();
  }

  default Boolean isFileSkipped() {
    return false;
  }

  //TODO think of way to include setting schema retriver in config only
  void setSchemaRetriever(SchemaRetriever schemaRetriever);

  List<SourceRecord> processFile(FileMetaData fileMetaData);
}
