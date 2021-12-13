package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class PubSubConfig extends PubSubGcsSourceConnectorTaskConfig {
  public static final String PUB_SUB_MESSAGE_MAX_NUMBER = "pubSubMessageMaxMessages";
  public static final Integer PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_DEFAULT = 1;
  public static final String PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME = "pubSubDeadLetterSubs";
  public static final String PUB_SUB_ACK_DEADLINE = "pubSubAckDeadline";
  public static final Integer PUB_SUB_ACK_DEADLINE_DEFAULT = 600;
  public static final String PUB_SUB_MESSAGE_FORMAT = "pubSubMessageFormat";
  public static final String PUB_SUB_MESSAGE_FORMAT_DEFAULT = "GCSPUBSUB";
  private static final ConfigDef.Type PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_TYPE = ConfigDef.Type.LONG;
  private static final ConfigDef.Importance PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_DOC = "Maximum no of messages per pull of pubsub. default is 1";
  private static final ConfigDef.Type PUB_SUB_ACK_DEADLINE_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Validator PUB_SUB_ACK_DEADLINE_VALIDATOR = ConfigDef.Range.atLeast(100);
  private static final ConfigDef.Importance PUB_SUB_ACK_DEADLINE_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String PUB_SUB_ACK_DEADLINE_DOC = "acknowledgement deadline for pubsub, if file is not processed within the deadline defined" +
      "it will be re-delivered to different task";
  private static final ConfigDef.Type PUB_SUB_MESSAGE_FORMAT_TYPE = ConfigDef.Type.STRING;
  // private static final ConfigDef.Validator PUB_SUB_MESSAGE_FORMAT_VALIDATOR = ConfigDef.LambdaValidator.with((s, o) -> )
  private static final ConfigDef.Importance PUB_SUB_MESSAGE_FORMAT_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String PUB_SUB_MESSAGE_FORMAT_DOC = "json form of message that pubsub gets." +
      " if it's GCS notification into PUBSUB then simply use GCSPUBSUB";

  public PubSubConfig(Map<String, String> originalProperties) {
    super(configDef(), originalProperties);
//    validate();
  }

  public PubSubConfig(ConfigDef configDef, Map<String, String> properties) {

    super(configDef(configDef), properties);
  }

  public static ConfigDef configDef() {
    ConfigDef configDef = new ConfigDef();
    return configDef(configDef);
  }

  public static ConfigDef configDef(ConfigDef configDef) {
    defineRequiredConf(configDef);
    return configDef;
  }

  static void defineRequiredConf(ConfigDef configDef) {
    configDef.define(PUB_SUB_ACK_DEADLINE, PUB_SUB_ACK_DEADLINE_TYPE, PUB_SUB_ACK_DEADLINE_DEFAULT, PUB_SUB_ACK_DEADLINE_VALIDATOR,
        PUB_SUB_ACK_DEADLINE_IMPORTANCE, PUB_SUB_ACK_DEADLINE_DOC);
    configDef.define(PUB_SUB_MESSAGE_FORMAT, PUB_SUB_MESSAGE_FORMAT_TYPE, PUB_SUB_MESSAGE_FORMAT_DEFAULT,
        PUB_SUB_MESSAGE_FORMAT_IMPORTANCE, PUB_SUB_MESSAGE_FORMAT_DOC);
    configDef.define(PUB_SUB_MESSAGE_MAX_NUMBER, PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_TYPE, PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_DEFAULT
        , PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_IMPORTANCE, PUB_SUB_MESSAGE_MAX_SIZE_NUMBER_DOC);
  }

  public static void validate(Map<String, String> props) {
  }
}
