package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class BlobReaderConfig extends PubSubGcsSourceConnectorTaskConfig {

  public static final String TOPIC_CONF = "topics";
  public static final String FILE_PROCESS_TIMEOUT = "fileProcessTimeout";
  public static final Integer FILE_PROCESS_TIMEOUT_DEFAULT = 600;
  public static final String SKIP_BAD_RECORDS = "skipBadRecords";
  public static final String PROJECT_CONFIG = "gcsProject";
  private static final ConfigDef.Type TOPIC_CONF_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance TOPIC_CONF_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String TOPIC_CONF_DOC = "Topic name into which the records needs to be pushed. if data needs to be pushed into " +
      "multiple topics please pass json object with hint as key and value as the topic name";
  private static final ConfigDef.Type FILE_PROCESS_TIMEOUT_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Validator FILE_PROCESS_TIMEOUT_VALIDATOR = ConfigDef.Range.atLeast(100);
  private static final ConfigDef.Importance FILE_PROCESS_TIMEOUT_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String FILE_PROCESS_TIMEOUT_DOC = "Time after which file process needs to timeout as per acknowledgement deadline to avoid " +
      "duplicate data into Kafka. This makes sense only in Async pull mode, In sync pull mode the connector expects to process all the file" +
      " completely with out timeout";
  String topic = null;
  Map<String, String> topicMap = null;
  String schemaSubject = null;
  Map<String, String> schemaSubjectMap = null;

  public BlobReaderConfig(Map<String, String> originalProperties) {
    super(configDef(), originalProperties);
//    validate();
  }

  public BlobReaderConfig(ConfigDef configDef, Map<String, String> properties) {
    super(configDef(configDef), properties);
  }

  public static ConfigDef configDef() {
    ConfigDef configDef = new ConfigDef();
    defineRequiredConf(configDef);
    return configDef;
  }

  static void defineRequiredConf(ConfigDef configDef) {
    configDef.define(TOPIC_CONF, TOPIC_CONF_TYPE, TOPIC_CONF_IMPORTANCE, TOPIC_CONF_DOC);
    configDef.define(FILE_PROCESS_TIMEOUT, FILE_PROCESS_TIMEOUT_TYPE, FILE_PROCESS_TIMEOUT_DEFAULT, FILE_PROCESS_TIMEOUT_VALIDATOR,
        FILE_PROCESS_TIMEOUT_IMPORTANCE, FILE_PROCESS_TIMEOUT_DOC);
  }

  public static ConfigDef configDef(ConfigDef configDef) {
    defineRequiredConf(configDef);
    return configDef;
  }

  public static void validate(Map<String, String> props) {
  }

  public String getTopicName() {
    if (this.topic == null && this.topicMap == null) {
      this.initializeTopic();
    }
    if (topic != null) {
      return topic;
    } else {
      Optional<Map.Entry<String, String>> firstEntry = topicMap.entrySet().stream().findFirst();
      if (firstEntry.isPresent()) {
        return firstEntry.get().getValue();
      } else {
        throw new ConfigException("Topics can't be extracted");
      }
    }
  }

  private void initializeTopic() {
    String rawTopicString = this.getString(TOPIC_CONF);
    if (rawTopicString.startsWith("{")) {
      JsonObject topicJsonObject = JsonParser.parseString(rawTopicString).getAsJsonObject();
      this.topicMap = topicJsonObject.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAsString()));
    } else {
      this.topic = rawTopicString;
    }
  }

  public String getTopicName(String hint) {
    if (this.topic == null && this.topicMap == null) {
      this.initializeTopic();
    }
    if (this.topicMap != null && this.topicMap.containsKey(hint)) {
      return this.topicMap.get(hint);
    } else {
      return topic;
    }
  }

  private void initializeSchemaSubject() {
    String rawSchemaSubject = this.getString(SCHEMA_SUBJECT);

    // single schema subject in this connector
    if (!rawSchemaSubject.startsWith("{")) {
      this.schemaSubject = rawSchemaSubject;
    } else {
      JsonObject schemaSubjectJsonObject = JsonParser.parseString(rawSchemaSubject).getAsJsonObject();
      this.schemaSubjectMap = schemaSubjectJsonObject.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAsString()));
    }
  }

  public String getSchemaSubject(String hint) {
    if (this.schemaSubject == null && this.schemaSubjectMap == null) {
      this.initializeSchemaSubject();
    }

    return (this.schemaSubjectMap != null && this.schemaSubjectMap.containsKey(hint)) ?
        this.schemaSubjectMap.get(hint) : this.schemaSubject;
  }

}
