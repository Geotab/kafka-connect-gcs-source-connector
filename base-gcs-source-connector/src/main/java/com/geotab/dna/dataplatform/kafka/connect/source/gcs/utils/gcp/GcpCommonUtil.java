package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PROJECT_CONFIG;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.exceptions.GcpResourceException;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class GcpCommonUtil {

  private static volatile GcpCommonUtil gcpCommonUtil;
  private final int PUBSUB_TOPIC_CLIENT_SHUTDOWN_WAITING_TIME = 10;
  private CredentialProvider credentialProvider;
  private AbstractConfig config;

  private GcpCommonUtil(AbstractConfig config, CredentialProvider credentialProvider) {
    this.config = config;
    this.credentialProvider = credentialProvider;
  }

  public static GcpCommonUtil getGcpCommonUtil(AbstractConfig config, CredentialProvider credentialProvider) {
    if (null == gcpCommonUtil) {
      synchronized (GcpCommonUtil.class) {
        if (null == gcpCommonUtil) {
          gcpCommonUtil = new GcpCommonUtil(config, credentialProvider);
        }
      }
    }

    return gcpCommonUtil;
  }

  public Storage getGcsStorage() {
    String projectId = this.config.getString(PROJECT_CONFIG);
    GoogleCredentials googleCredentials = this.credentialProvider.getCredentials();

    return StorageOptions.newBuilder().setProjectId(projectId).setCredentials(googleCredentials).build().getService();
  }


  public Logging getCloudLogging() {
    String projectId = this.config.getString(PROJECT_CONFIG);
    GoogleCredentials googleCredentials = this.credentialProvider.getCredentials();
    return LoggingOptions.newBuilder().setProjectId(projectId).setCredentials(googleCredentials).build().getService();
  }

  public ServiceAccount getServiceAccount() {
    String projectId = this.config.getString(PROJECT_CONFIG);
    Storage storage = this.getGcsStorage();

    return storage.getServiceAccount(projectId);
  }

  public TopicAdminClient getTopicAdminClient() {
    GoogleCredentials googleCredentials = this.credentialProvider.getCredentials();

    try {
      TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials)).build();
      return TopicAdminClient.create(topicAdminSettings);
    } catch (IOException ioe) {
      throw new GcpResourceException(ioe.getMessage(), ioe.getCause());
    }
  }

}
