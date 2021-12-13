package com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileSystemType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.common.config.AbstractConfig;

public interface FileSystem {
  // void setCredentials(GoogleCredentials credentials);
  void initialize(AbstractConfig config);

  BufferedReader getBufferedReader(FileMetaData fileMetaData) throws IOException;

  InputStream getInputStream(FileMetaData fileMetaData);

  void copyBlob(FileMetaData source, FileMetaData destination) throws IOException;

  void moveBlob(FileMetaData source, FileMetaData destination) throws IOException;

  void deleteBlob(FileMetaData file) throws IOException;

  FileSystemType getType();
}
