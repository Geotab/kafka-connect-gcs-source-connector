package com.geotab.dna.dataplatform.kafka.connect.source.gcs.executor;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.ProcessorReturnObject;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.reader.BlobReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

@Slf4j
@Getter
public class DefaultThread implements Runnable {
  private final BlobReader blobReader;
  private final FileMetaData fileMetaData;
  private final ConcurrentLinkedQueue<ProcessorReturnObject> returnObjects;


  public DefaultThread(BlobReader blobReader, FileMetaData fileMetaData, ConcurrentLinkedQueue<ProcessorReturnObject> returnObjects) {
    this.blobReader = blobReader;
    this.fileMetaData = fileMetaData;
    this.returnObjects = returnObjects;
  }

  @Override
  public void run() {
    ProcessorReturnObject returnObject = this.processFile();
    this.returnObjects.offer(returnObject);
  }

  protected ProcessorReturnObject processFile() {
    log.info("in the processFile, processing {}", fileMetaData.getPath());
    List<SourceRecord> extractedRecords = new LinkedList<>();
    Map<String, Object> metrics = new HashMap<>();
    boolean isFailed = false;
    boolean isSkipped = false;
    try {
      extractedRecords = blobReader.processFile(fileMetaData);
      isSkipped = blobReader.isFileSkipped();
      metrics = blobReader.getMetrics();
      log.info("successfully processed {}", fileMetaData.getPath());
    } catch (Exception e) {
      isFailed = true;
      log.error("exception in file processing, file name: {}, bucket: {}, {}", fileMetaData.getPath(), fileMetaData.getBucket(), e);
    }

    return new ProcessorReturnObject(extractedRecords, isFailed, isSkipped, this.fileMetaData, metrics);
  }
}
