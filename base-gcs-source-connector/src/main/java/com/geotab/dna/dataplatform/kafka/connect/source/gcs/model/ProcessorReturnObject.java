package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.apache.kafka.connect.source.SourceRecord;

@Data
public class ProcessorReturnObject {
  private Boolean isFailed;
  private Boolean isSKipped;
  private FileMetaData fileMetaData;
  private List<SourceRecord> fileData;
  private Map<String, Object> metrics;

  public ProcessorReturnObject(List<SourceRecord> fileData, Boolean isFailed, Boolean isSKipped,
                               FileMetaData fileMetaData, Map<String, Object> metrics) {
    this.fileData = new LinkedList<>(fileData);
    this.isFailed = isFailed;
    this.isSKipped = isSKipped;
    this.fileMetaData = fileMetaData;
    this.metrics = metrics;
  }
}
