package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.BlobReaderConfig;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class CredentialProvider {
  private static volatile CredentialProvider credentialProvider;
  private final String credentialScope = "https://www.googleapis.com/auth/cloud-platform";
  private AbstractConfig config;

  private CredentialProvider(AbstractConfig config) {
    this.config = config;
  }

  public static CredentialProvider getCredentialProvider(AbstractConfig config) {
    if (null == credentialProvider) {
      synchronized (CredentialProvider.class) {
        if (null == credentialProvider) {
          credentialProvider = new CredentialProvider(config);
        }
      }
    }

    return credentialProvider;
  }

  public GoogleCredentials getCredentials() {
    String keySource = this.config.getString(BlobReaderConfig.KEY_SOURCE_CONFIG);
    String keyFile = this.config.getString(BlobReaderConfig.KEYFILE_CONFIG);
    try {
      switch (keySource) {
        case "JSON":
          return GoogleCredentials.fromStream(new ByteArrayInputStream(keyFile.getBytes(StandardCharsets.UTF_8))).createScoped(this.credentialScope);
        case "FILE":
          return GoogleCredentials.fromStream(new FileInputStream(keyFile)).createScoped(this.credentialScope);
        default:
          return GoogleCredentials.getApplicationDefault().createScoped(this.credentialScope);

      }
    } catch (IOException e) {
      log.error("cannot get credentials from the key source with {} {}", e.getMessage(), e.getCause());
      return null;
    }
  }
}
