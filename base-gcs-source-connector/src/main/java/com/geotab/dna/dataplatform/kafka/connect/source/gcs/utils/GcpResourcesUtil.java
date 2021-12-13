package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp.GcpCommonUtil;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp.GcsUtil;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp.PubSubSubscriptionUtil;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp.PubSubTopicUtil;
import org.apache.kafka.common.config.AbstractConfig;

public class GcpResourcesUtil {
  protected GcsUtil gcsUtil;
  protected PubSubSubscriptionUtil pubSubSubscriptionUtil;
  protected PubSubTopicUtil pubSubTopicUtil;

  public GcpResourcesUtil(AbstractConfig config) {
    CredentialProvider credentialProvider = CredentialProvider.getCredentialProvider(config);
    GcpCommonUtil gcpCommonUtil = GcpCommonUtil.getGcpCommonUtil(config, credentialProvider);
    this.gcsUtil = new GcsUtil(credentialProvider, config, new ObjectMapper(), gcpCommonUtil);
    this.pubSubSubscriptionUtil = new PubSubSubscriptionUtil(config, credentialProvider, gcpCommonUtil);
    this.pubSubTopicUtil = new PubSubTopicUtil(credentialProvider, config, gcpCommonUtil);
  }

  public void createGcpResources() {
    this.gcsUtil.createIfNotExitsGcsBucket();
    this.gcsUtil.createIfNotExistsGcsArchiveBucket();
    this.gcsUtil.createIfNotExistsGcsErrorBucket();
    this.pubSubTopicUtil.createIfNotExitsPubSubTopics();
    this.pubSubTopicUtil.createIfNotExistsDeadLetterTopic();
    this.pubSubSubscriptionUtil.createIfNotExitsTopicSubscription();
    this.pubSubSubscriptionUtil.createIfNotExitsDeadLetterSubscription();
    this.gcsUtil.createPubSubNotification();
  }

}
