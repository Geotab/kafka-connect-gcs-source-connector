package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.pubsub;

public enum NotificationType {
  OBJECT_FINALIZE,
  OBJECT_METADATA_UPDATE,
  OBJECT_DELETE,
  OBJECT_ARCHIVE;
}
