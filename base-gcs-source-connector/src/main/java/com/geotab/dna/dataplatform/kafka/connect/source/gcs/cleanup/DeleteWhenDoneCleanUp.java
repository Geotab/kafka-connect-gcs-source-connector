package com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.FileSystem;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.FileSystemService;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileSystemType;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class DeleteWhenDoneCleanUp implements CleanUp {
  private FileSystem gcsFileSystem;

  @Override
  public boolean cleanUpBlob(FileMetaData fileMetaData) {
    try {
      this.gcsFileSystem.deleteBlob(fileMetaData);
      return true;
    } catch (IOException e) {
      log.error("cannot delete file {}", fileMetaData.getFileID());
      return false;
    }
  }

  @Override
  public void configure(AbstractConfig config) {
    FileSystemService fileSystemService = new FileSystemService(config);
    this.gcsFileSystem = fileSystemService.getFileSystem(FileSystemType.GCS);
  }
}
