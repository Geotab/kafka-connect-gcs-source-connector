package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup.ArchiveToBucketCleanUp;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup.CleanUp;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup.DeleteWhenDoneCleanUp;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup.DoNothingCleanUp;
import lombok.Getter;

@Getter
public enum CleanUpStrategy {
  DO_NOTHING(DoNothingCleanUp.class),
  DELETE_WHEN_DONE(DeleteWhenDoneCleanUp.class),
  ARCHIVE_TO_BUCKET(ArchiveToBucketCleanUp.class);

  private final Class<? extends CleanUp> cleanUpStrategyClass;

  CleanUpStrategy(Class<? extends CleanUp> cleanUpStrategyClass) {
    this.cleanUpStrategyClass = cleanUpStrategyClass;
  }
}
