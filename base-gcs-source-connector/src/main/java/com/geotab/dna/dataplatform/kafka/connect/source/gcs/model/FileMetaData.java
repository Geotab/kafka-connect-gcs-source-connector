package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@Setter
@ToString
@Builder
public class FileMetaData implements Serializable {

  private final String fileID;
  @Builder.Default
  private final FileSystemType fileSystemType = FileSystemType.GCS;
  //  private final URI uri;
  private final String bucket;
  private final String path;
  private final Long contentLength;
  private final Long lastModified;
  private final Map<String, Object> userDefinedMetadata;
  private final long pulledAtEpochTime;
  private final byte[] data;

  public void addCustomMetaData(final String key, final Object value) {
    this.userDefinedMetadata.put(key, value);
  }

  public FileMetaData constructFileMetadataWithDest(String destinationBucket) {
    return FileMetaData.builder()
        .fileID(this.fileID)
        .fileSystemType(this.fileSystemType)
        .bucket(destinationBucket)
        .path(this.path)
        .contentLength(this.contentLength)
        .lastModified(this.lastModified)
        .userDefinedMetadata(this.userDefinedMetadata).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileMetaData that = (FileMetaData) o;
    return Objects.equals(fileID, that.fileID)
        && Objects.equals(fileSystemType, that.fileSystemType)
        && Objects.equals(contentLength, that.contentLength)
        && Objects.equals(lastModified, that.lastModified)
        && Objects.equals(userDefinedMetadata, that.userDefinedMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileID, fileSystemType, contentLength);
  }
}
