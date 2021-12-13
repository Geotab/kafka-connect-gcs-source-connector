package com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import java.util.List;
import java.util.Map;

public interface BaseScanner {
  public abstract void configure(Map<String, String> originalConfigs);

  public abstract List<FileMetaData> poll();

  public abstract void startPoll();

  public abstract void stopPoll();

  public abstract void ackMessage(FileMetaData fileMetaData);

  public abstract void nackMessage(FileMetaData fileMetaData);
}
