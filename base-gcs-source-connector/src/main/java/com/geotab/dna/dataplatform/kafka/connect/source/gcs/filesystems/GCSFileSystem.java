package com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileSystemType;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp.GcpCommonUtil;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class GCSFileSystem implements FileSystem {
  GcpCommonUtil gcpCommonUtil;
  Storage storage;

  @Override
  public void initialize(AbstractConfig config) {
    CredentialProvider credentialProvider = CredentialProvider.getCredentialProvider(config);
    this.gcpCommonUtil = GcpCommonUtil.getGcpCommonUtil(config, credentialProvider);
    this.storage = gcpCommonUtil.getGcsStorage();
  }

  @Override
  public BufferedReader getBufferedReader(FileMetaData fileMetaData) {
    String bucketName = fileMetaData.getBucket();
    String fileName = fileMetaData.getPath();
    Blob blob = storage.get(bucketName, fileName);
    if (blob == null) {
      log.error("File {} Doesn't exist in {}", fileMetaData.getPath(), bucketName);
      return null;
    }
    ReadChannel readChannel = blob.reader();
    return new BufferedReader(Channels.newReader(readChannel, StandardCharsets.UTF_8));
  }

  @Override
  public InputStream getInputStream(FileMetaData fileMetaData) {
    String bucketName = fileMetaData.getBucket();
    Blob blob = storage.get(bucketName, fileMetaData.getPath());
    if (blob == null) {
      log.error("File {} Doesn't exist in {}", fileMetaData.getPath(), bucketName);
      return null;
    }
    ReadChannel readChannel = blob.reader();
    return Channels.newInputStream(readChannel);
  }

  @Override
  public void copyBlob(FileMetaData source, FileMetaData destination) {
    String sourceBucket = source.getBucket();
    String destinationBucket = destination.getBucket();
    String path = source.getPath();
    String filename = path.substring(path.lastIndexOf("/") + 1);
    String filenameWithDate = filename.substring(0, 6) + "/" + filename;
    Blob blob = this.storage.get(sourceBucket, path);
    if (blob == null) {
      log.error("Error when copying {}: blob does not exist in {}", filename, sourceBucket);
    } else {
      blob.copyTo(destinationBucket, filenameWithDate);
    }
  }

  @Override
  public void moveBlob(FileMetaData source, FileMetaData destination) {
    String sourceBucket = source.getBucket();
    String destinationBucket = destination.getBucket();
    String path = source.getPath();
    String filename = path.substring(path.lastIndexOf("/") + 1);
    String filenameWithDate = filename.substring(0, 6) + "/" + filename;
    Blob blob = this.storage.get(sourceBucket, path);
    if (blob == null) {
      log.error("Error when moving {}: blob does not exist in {}", filename, sourceBucket);
    } else {
      blob.copyTo(destinationBucket, filenameWithDate);
      this.storage.delete(sourceBucket, path);
    }
  }

  @Override
  public void deleteBlob(FileMetaData file) {
    String bucket = file.getBucket();
    String filename = file.getPath();
    this.storage.delete(bucket, filename);
  }

  @Override
  public FileSystemType getType() {
    return FileSystemType.GCS;
  }

}
