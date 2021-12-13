package com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import org.apache.kafka.common.config.AbstractConfig;

public class DoNothingCleanUp implements CleanUp {
  @Override
  public boolean cleanUpBlob(FileMetaData fileMetaData) {
    return false;
  }

  @Override
  public void configure(AbstractConfig config) {
    // nothing to be done here
  }
}
