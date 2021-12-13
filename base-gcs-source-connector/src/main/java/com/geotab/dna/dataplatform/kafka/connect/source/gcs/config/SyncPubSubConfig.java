package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class SyncPubSubConfig extends PubSubConfig {
  public static final String PUB_SUB_MESSAGE_MAX_SIZE = "pubSubMessageMaxSize";
  public static final Integer PUB_SUB_MESSAGE_MAX_SIZE_DEFAULT = 20;

  private static final ConfigDef.Type PUB_SUB_MESSAGE_MAX_SIZE_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Importance PUB_SUB_MESSAGE_MAX_SIZE_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String PUB_SUB_MESSAGE_MAX_SIZE_DOC = "Maximum size of pubsub message. default is 20 MB";


  public SyncPubSubConfig(ConfigDef configDef, Map<String, String> originalProperties) {
    super(configDef, originalProperties);
  }


  public SyncPubSubConfig(Map<String, String> originalProperties) {
    this(addSyncConfig(), originalProperties);
  }

  public static ConfigDef addSyncConfig() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(PUB_SUB_MESSAGE_MAX_SIZE, PUB_SUB_MESSAGE_MAX_SIZE_TYPE, PUB_SUB_MESSAGE_MAX_SIZE_DEFAULT, PUB_SUB_MESSAGE_MAX_SIZE_IMPORTANCE
        , PUB_SUB_MESSAGE_MAX_SIZE_DOC);
    return configDef;
  }
}
