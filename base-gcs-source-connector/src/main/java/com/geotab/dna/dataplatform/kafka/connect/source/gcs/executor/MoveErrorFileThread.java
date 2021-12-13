package com.geotab.dna.dataplatform.kafka.connect.source.gcs.executor;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.GCS_ERROR_BUCKET_NAME;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.FileSystem;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner.BaseScanner;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class MoveErrorFileThread implements Runnable {

  private BaseScanner deadLetterScanner;
  private FileSystem fileSystem;
  private AbstractConfig config;

  public MoveErrorFileThread(BaseScanner deadLetterScanner, FileSystem fileSystem, AbstractConfig config) {
    this.deadLetterScanner = deadLetterScanner;
    this.fileSystem = fileSystem;
    this.config = config;
  }

  @Override
  public void run() {
    this.moveErrorFiles();
  }

  private void moveErrorFiles() {
    List<FileMetaData> errorFiles = deadLetterScanner.poll();
    if (null == errorFiles || errorFiles.isEmpty()) {
      return;
    }
    String errorBucket = config.getString(GCS_ERROR_BUCKET_NAME);
    for (FileMetaData fileMetaData : errorFiles) {
      try {
        this.fileSystem.moveBlob(fileMetaData, fileMetaData.constructFileMetadataWithDest(errorBucket));
      } catch (IOException e) {
        log.error("cannot move bucket {} due to {}", fileMetaData.getBucket(), e.getCause());
        deadLetterScanner.nackMessage(fileMetaData);
      }
      deadLetterScanner.ackMessage(fileMetaData);
      log.info("successfully move file {} to error bucket {}", fileMetaData.getPath(), errorBucket);
    }
  }
}