package com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import org.apache.kafka.common.config.AbstractConfig;

public interface CleanUp {
  public boolean cleanUpBlob(FileMetaData fileMetaData);

  public void configure(AbstractConfig config);
}
