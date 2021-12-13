package com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner;


import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.SyncPubSubConfig;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeadLetterSyncScanner extends SyncPubSubScanner {

  @Override
  public void initializeSubscriptionName(Map<String, String> originalConfigs) {
    SyncPubSubConfig syncPubSubConfig = new SyncPubSubConfig(originalConfigs);
    this.subscriptionName = syncPubSubConfig.getString(PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME);
  }

  @Override
  public void configure(Map<String, String> originalConfigs) {
    super.configure(originalConfigs);
  }
}
