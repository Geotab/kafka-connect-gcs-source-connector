package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Slf4j
public class PubSubGcsSourceConnectorConfig extends AbstractConfig {
  public static final String SCHEMA_REGISTRY_URL = "schemaRegistryUrl";
  public static final ConfigDef.Type SCHEMA_REGISTRY_URL_TYPE = ConfigDef.Type.STRING;
  public static final ConfigDef.Importance SCHEMA_REGISTRY_URL_IMPORTANCE = ConfigDef.Importance.HIGH;
  public static final String SCHEMA_REGISTRY_URL_DOC = "Schema Registry URL from which we need to retrieve schema";
  public static final String SCHEMA_REGISTRY_URL_GROUP = "Queue";
  public static final int SCHEMA_REGISTRY_URL_ORDER_IN_GROUP = 2;
  public static final ConfigDef.Width SCHEMA_REGISTRY_URL_WIDTH = ConfigDef.Width.LONG;
  public static final String SCHEMA_REGISTRY_URL_DISPLAY = "Schema Registry Url";
  public static final String SCHEMA_SUBJECT = "schemaSubjectName";
  public static final ConfigDef.Type SCHEMA_SUBJECT_TYPE = ConfigDef.Type.STRING;
  public static final ConfigDef.Importance SCHEMA_SUBJECT_IMPORTANCE = ConfigDef.Importance.HIGH;
  public static final String SCHEMA_SUBJECT_DOC = "Schema Registry subject name from which we need to retrieve schema";
  public static final String SCHEMA_SUBJECT_GROUP = "Queue";
  public static final int SCHEMA_SUBJECT_ORDER_IN_GROUP = 2;
  public static final ConfigDef.Width SCHEMA_SUBJECT_WIDTH = ConfigDef.Width.LONG;
  public static final String SCHEMA_SUBJECT_DISPLAY = "Schema Registry Subject Name";
  public static final String SCHEMA_SUBJECT_VERSION = "schemaSubjectVersion";
  public static final ConfigDef.Type SCHEMA_SUBJECT_VERSION_TYPE = ConfigDef.Type.STRING;
  public static final ConfigDef.Importance SCHEMA_SUBJECT_VERSION_IMPORTANCE = ConfigDef.Importance.HIGH;
  public static final String SCHEMA_SUBJECT_VERSION_DOC = "Schema Registry subject version from which we need to retrieve schema";
  public static final String SCHEMA_SUBJECT_VERSION_GROUP = "Queue";
  public static final int SCHEMA_SUBJECT_VERSION_ORDER_IN_GROUP = 3;
  public static final ConfigDef.Width SCHEMA_SUBJECT_VERSION_WIDTH = ConfigDef.Width.LONG;
  public static final String SCHEMA_SUBJECT_VERSION_DISPLAY = "Schema Registry Subject Version";


  public static final String KEYFILE_CONFIG = "gcsKeyfile";
  public static final String KEY_SOURCE_CONFIG = "gcsKeySource";
  public static final String KEY_SOURCE_DEFAULT = "";
  public static final String PROJECT_CONFIG = "gcsProject";
  public static final String PROJECT_LOCATION = "projectLocation";
  public static final String GCS_BUCKET_NAME = "gcsBucketName";
  public static final String PUB_SUB_TOPIC_NAME = "pubSubTopic";
  public static final String PUB_SUB_TOPIC_SUBSCRIPTION_NAME = "pubSubSubscription";
  public static final String PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME = "pubSubDeadLetterSubs";
  public static final String PUB_SUB_DEAD_LETTER_TOPIC_NAME = "pubSubDeadLetterTopic";
  public static final String PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE = "autoCreateDeadLetterTopic";
  public static final String BUCKET_CREATE_CONFIG = "autoCreateBucket";
  public static final boolean BUCKET_CREATE_DEFAULT = true;
  public static final String ERROR_BUCKET_CREATE_CONFIG = "autoCreateErrorBucket";
  public static final boolean ERROR_BUCKET_CREATE_CONFIG_DEFAULT = true;
  public static final String ARCHIVE_BUCKET_CREATE_CONFIG = "autoCreateArchiveBucket";
  public static final boolean ARCHIVE_BUCKET_CREATE_CONFIG_DEFAULT = true;
  public static final String PUB_SUB_TOPIC_CREATE_CONFIG = "autoCreatePubSubTopic";
  public static final boolean PUB_SUB_TOPIC_CREATE_DEFAULT = true;
  public static final String PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_CONFIG = "autoCreatePubSubSubscription";
  public static final boolean PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_DEFAULT = true;
  public static final String BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG = "autoCreateBucketNotification";
  public static final String PUB_SUB_BACK_OFF_TIME = "pubSubBackOffTime";
  public static final Integer PUB_SUB_BACK_OFF_TIME_DEFAULT = 100;
  public static final String PUB_SUB_MESSAGE_HOLDING_DEADLINE = "pubSubMessageHoldingDeadline";
  public static final String PUB_SUB_MAX_RETRY = "pubSubMaxRetry";
  public static final Integer PUB_SUB_MAX_RETRY_DEFAULT = 5;
  public static final String GCS_RETRY_CONFIG = "gcsBucketRetry";
  public static final Integer GCS_RETRY_DEFAULT = 0;
  //TODO check if this is necessary
  public static final ConfigDef.Type PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_TYPE = ConfigDef.Type.STRING;
  public static final ConfigDef.Importance PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_DOC = "name of subscription in pubsub deadletter topic";
  public static final String PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_DEFAULT = "";
  public static final String STORAGE_RETRY_WAIT_CONFIG = "storageRetryWait";
  public static final Long STORAGE_RETRY_WAIT_DEFAULT = 1000L;
  public static final String FILE_PROCESS_RETRY = "fileProcessRetry";
  public static final Integer FILE_PROCESS_RETRY_DEFAULT = 0;
  public static final String SKIP_BAD_RECORDS = "skipBadRecords";
  public static final boolean SKIP_BAD_RECORDS_DEFAULT = false;
  public static final String CLEAN_UP_STRATEGY = "cleanupStrategy";
  public static final String CLEAN_UP_STRATEGY_DEFAULT = "DO_NOTHING";
  public static final String GCS_ARCHIVE_BUCKET_NAME = "gcsArchiveBucketName";
  public static final String GCS_ERROR_BUCKET_NAME = "gcsErrorBucketName";

  public static final String PROCESS_CORE_THREAD_POOL_SIZE = "coreThreadPoolSize";
  public static final String PROCESS_MAX_THREAD_POOL_SIZE = "maxThreadPoolSize";
  public static final String PUB_SUB_MESSAGE_ACK_DEADLINE = "pubSubMessageAckDeadline";
  public static final Integer PUB_SUB_MESSAGE_ACK_DEADLINE_DEFAULT = 600;
  static final String PUB_SUB_BACK_OFF_TIME_DOC = "time pubsub needs to wait before trying to redeliver notifications to different task";
  static final String CLEAN_UP_STRATEGY_DOC = "define what needs to be done once processing is complete and the file is processed.";
  private static final String CONNECT_NAME_CONFIG = "name";
  private static final ConfigDef.Type KEYFILE_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance KEYFILE_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String KEYFILE_DOC = "The file containing a JSON key with BigQuery service account credentials";
  private static final ConfigDef.Type KEY_SOURCE_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Validator KEY_SOURCE_VALIDATOR = ConfigDef.ValidString.in("", "FILE", "JSON");
  private static final ConfigDef.Importance KEY_SOURCE_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String KEY_SOURCE_DOC = "Determines whether the keyfile config is the path to the credentials json, or the json itself";
  private static final ConfigDef.Type PROJECT_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PROJECT_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PROJECT_DOC = "The GCS project in which bucket reside";
  private static final ConfigDef.Type PROJECT_LOCATION_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PROJECT_LOCATION_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PROJECT_LOCATION_DOC = "The GCS, PubSub resource zone";
  private static final ConfigDef.Type GCS_BUCKET_NAME_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance GCS_BUCKET_NAME_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String GCS_BUCKET_NAME_DOC = "The name of the bucket from which data needs to be loaded.";
  private static final ConfigDef.Type PUB_SUB_TOPIC_NAME_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PUB_SUB_TOPIC_NAME_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PUB_SUB_TOPIC_NAME_DOC = "Name of topic from which pubsub needs to pull notifications from";
  private static final ConfigDef.Type PUB_SUB_TOPIC_SUBSCRIPTION_NAME_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PUB_SUB_TOPIC_SUBSCRIPTION_NAME_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PUB_SUB_TOPIC_SUBSCRIPTION_NAME_DOC = "name of subscription in pub sub that we get alerts and metadata based " +
      "on we need to process files";
  private static final ConfigDef.Type PUB_SUB_DEAD_LETTER_TOPIC_NAME_TYPE = ConfigDef.Type.STRING;
  private static final Object PUB_SUB_DEAD_LETTER_TOPIC_NAME_DEFAULT = "";
  private static final ConfigDef.Importance PUB_SUB_DEAD_LETTER_TOPIC_NAME_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PUB_SUB_DEAD_LETTER_TOPIC_NAME_DOC = "The name of the bucket in which gcs blobs used to batch load to BigQuery "
      + "should be located. Only relevant if enableBatchLoad is configured.";
  private static final ConfigDef.Type PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_TYPE = ConfigDef.Type.BOOLEAN;
  private static final Object PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_DEFAULT = true;
  private static final ConfigDef.Importance PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_DOC = "Automatically create the deadletter topic in pubsub";
  private static final ConfigDef.Type BUCKET_CREATE_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance BUCKET_CREATE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String BUCKET_CREATE_DOC = "Automatically create gcs bucket if they don't already exist";
  private static final ConfigDef.Type ERROR_BUCKET_CREATE_CONFIG_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance ERROR_BUCKET_CREATE_CONFIG_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String ERROR_BUCKET_CREATE_CONFIG_DOC = "Automatically create gcs error bucket if it is not existed";
  private static final ConfigDef.Type ARCHIVE_BUCKET_CREATE_CONFIG_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance ARCHIVE_BUCKET_CREATE_CONFIG_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String ARCHIVE_BUCKET_CREATE_CONFIG_DOC = "Automatically create gcs archive bucket if it is not existed";
  private static final ConfigDef.Type PUB_SUB_TOPIC_CREATE_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance PUB_SUB_TOPIC_CREATE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PUB_SUB_TOPIC_CREATE_DOC = "Automatically create pub sub topic if they don't already exist";
  private static final ConfigDef.Type PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_DOC = "Automatically create Pub Sub subscription if they don't already exist";
  private static final boolean BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_DEFAULT = true;
  private static final ConfigDef.Type BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_DOC = "Automatically create bucket notification to the pub sub";
  private static final ConfigDef.Type PUB_SUB_BACK_OFF_TIME_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Validator PUB_SUB_BACK_OFF_TIME_VALIDATOR = ConfigDef.Range.atLeast(10);
  private static final ConfigDef.Importance PUB_SUB_BACK_OFF_TIME_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final Integer PUB_SUB_MESSAGE_HOLDING_DEADLINE_DEFAULT = 600;
  private static final ConfigDef.Type PUB_SUB_MESSAGE_HOLDING_DEADLINE_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Importance PUB_SUB_MESSAGE_HOLDING_DEADLINE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PUB_SUB_MESSAGE_HOLDING_DEADLINE_DOC = "the longest time a message can be hold in a connector";
  private static final ConfigDef.Type PUB_SUB_MAX_RETRY_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Validator PUB_SUB_MAX_RETRY_VALIDATOR = ConfigDef.Range.atLeast(5);
  private static final ConfigDef.Importance PUB_SUB_MAX_RETRY_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String PUB_SUB_MAX_RETRY_DOC = "no of retries pubsub needs to try before dead letting notifications";
  private static final ConfigDef.Type PUB_SUB_MESSAGE_ACK_DEADLINE_TYPE = ConfigDef.Type.INT;
  // private static final ConfigDef.Validator PUB_SUB_MESSAGE_ACK_DEADLINE = ConfigDef.LambdaValidator.with((s, o) -> )
  private static final ConfigDef.Importance PUB_SUB_MESSAGE_ACK_DEADLINE_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String PUB_SUB_MESSAGE_ACK_DEADLINE_DOC = "Maximum no of messages per pull of pubsub. default is 1";
  private static final ConfigDef.Type GCS_RETRY_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Validator GCS_RETRY_VALIDATOR = ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance GCS_RETRY_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String GCS_RETRY_DOC = "The number of retry attempts that will be made to read blob before failing the task";
  private static final ConfigDef.Type STORAGE_RETRY_WAIT_CONFIG_TYPE = ConfigDef.Type.LONG;
  private static final ConfigDef.Validator STORAGE_RETRY_WAIT_VALIDATOR = ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance STORAGE_RETRY_WAIT_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String STORAGE_RETRY_WAIT_DOC = "The minimum amount of time, in milliseconds, to wait between BigQuery backend or quota "
      + "exceeded error retry attempts.";
  private static final ConfigDef.Type FILE_PROCESS_RETRY_TYPE = ConfigDef.Type.LONG;
  private static final ConfigDef.Validator FILE_PROCESS_RETRY_VALIDATOR = ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance FILE_PROCESS_RETRY_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String FILE_PROCESS_RETRY_DOC = "The number of retry attempts that will be made to process blob before failing the task";
  private static final ConfigDef.Type SKIP_BAD_RECORDS_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance SKIP_BAD_RECORDS_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String SKIP_BAD_RECORDS_DOC = "flag to decide weather to skip bad records and process file or fail the file if there is some" +
      "is at-least one bad record";
  private static final ConfigDef.Type CLEAN_UP_STRATEGY_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Validator CLEAN_UP_STRATEGY_VALIDATOR =
      ConfigDef.ValidString.in("DO_NOTHING", "DELETE_WHEN_DONE", "ARCHIVE_TO_BUCKET");
  private static final ConfigDef.Importance CLEAN_UP_STRATEGY_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final ConfigDef.Type GCS_ARCHIVE_BUCKET_NAME_TYPE = ConfigDef.Type.STRING;
  private static final Object GCS_ARCHIVE_BUCKET_NAME_DEFAULT = "";
  private static final ConfigDef.Importance GCS_ARCHIVE_BUCKET_NAME_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String GCS_ARCHIVE_BUCKET_NAME_DOC = "The name of the bucket in which processed gcs blobs are stored as part of cleanup.";
  private static final ConfigDef.Type GCS_ERROR_BUCKET_NAME_TYPE = ConfigDef.Type.STRING;
  private static final Object GCS_ERROR_BUCKET_NAME_DEFAULT = "";
  private static final ConfigDef.Importance GCS_ERROR_BUCKET_NAME_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String GCS_ERROR_BUCKET_NAME_DOC = "The name of the bucket in which problematic gcs blobs are stored as part of cleanup.";

  private static final ConfigDef.Type PROCESS_CORE_THREAD_POOL_SIZE_TYPE = ConfigDef.Type.INT;
  private static final Integer PROCESS_CORE_THREAD_POOL_SIZE_DEFAULT = 3;
  private static final ConfigDef.Importance PROCESS_CORE_THREAD_POOL_SIZE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final ConfigDef.Validator PROCESS_CORE_THREAD_POOL_SIZE_VALIDATOR = ConfigDef.Range.between(1, 10);
  private static final String PROCESS_CORE_THREAD_POOL_SIZE_DOC = "no of threads that needs to be started for processing";
  private static final ConfigDef.Type PROCESS_MAX_THREAD_POOL_SIZE_TYPE = ConfigDef.Type.INT;
  private static final Integer PROCESS_MAX_THREAD_POOL_SIZE_DEFAULT = 3;
  private static final ConfigDef.Importance PROCESS_MAX_THREAD_POOL_SIZE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final ConfigDef.Validator PROCESS_MAX_THREAD_POOL_SIZE_VALIDATOR = ConfigDef.Range.between(1, 10);
  private static final String PROCESS_MAX_THREAD_POOL_SIZE_DOC = "max no of threads that needs to be started for processing files.";
  private static ConfigDef configDef;

  public PubSubGcsSourceConnectorConfig(Map<String, String> originalProperties) {
    super(configDef(), originalProperties);
//    validate();
  }

  public PubSubGcsSourceConnectorConfig(ConfigDef configDef, Map<String, String> properties) {
    super(configDef(configDef), properties);
  }

  public static String getConnectorNameConfig() {
    return CONNECT_NAME_CONFIG;
  }

  public static ConfigDef configDef() {
    final ConfigDef configDef = new ConfigDef();
    addQueueConfigGroup(configDef);
    addGCPConfig(configDef);
    addFileProcessConfig(configDef);
    return configDef;
  }

  static ConfigDef configDef(ConfigDef configDef) {
    addQueueConfigGroup(configDef);
    addGCPConfig(configDef);
    addFileProcessConfig(configDef);
    return configDef;
  }


  static void addFileProcessConfig(ConfigDef configDef) {

    configDef.define(CLEAN_UP_STRATEGY, CLEAN_UP_STRATEGY_TYPE, CLEAN_UP_STRATEGY_DEFAULT, CLEAN_UP_STRATEGY_VALIDATOR, CLEAN_UP_STRATEGY_IMPORTANCE,
        CLEAN_UP_STRATEGY_DOC);

    configDef.define(PROCESS_CORE_THREAD_POOL_SIZE, PROCESS_CORE_THREAD_POOL_SIZE_TYPE, PROCESS_CORE_THREAD_POOL_SIZE_DEFAULT,
        PROCESS_CORE_THREAD_POOL_SIZE_VALIDATOR,
        PROCESS_CORE_THREAD_POOL_SIZE_IMPORTANCE, PROCESS_CORE_THREAD_POOL_SIZE_DOC);
    configDef.define(PROCESS_MAX_THREAD_POOL_SIZE, PROCESS_MAX_THREAD_POOL_SIZE_TYPE, PROCESS_MAX_THREAD_POOL_SIZE_DEFAULT,
        PROCESS_MAX_THREAD_POOL_SIZE_VALIDATOR,
        PROCESS_MAX_THREAD_POOL_SIZE_IMPORTANCE, PROCESS_MAX_THREAD_POOL_SIZE_DOC);

  }

  static void addQueueConfigGroup(ConfigDef configDef) {

    configDef.define(SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_URL_TYPE, SCHEMA_REGISTRY_URL_IMPORTANCE, SCHEMA_REGISTRY_URL_DOC, SCHEMA_REGISTRY_URL_GROUP
        , SCHEMA_REGISTRY_URL_ORDER_IN_GROUP, SCHEMA_REGISTRY_URL_WIDTH, SCHEMA_REGISTRY_URL_DISPLAY);
    configDef.define(SCHEMA_SUBJECT, SCHEMA_SUBJECT_TYPE, SCHEMA_SUBJECT_IMPORTANCE, SCHEMA_SUBJECT_DOC, SCHEMA_SUBJECT_GROUP,
        SCHEMA_SUBJECT_ORDER_IN_GROUP, SCHEMA_SUBJECT_WIDTH, SCHEMA_SUBJECT_DISPLAY);
    configDef.define(SCHEMA_SUBJECT_VERSION, SCHEMA_SUBJECT_VERSION_TYPE, SCHEMA_SUBJECT_VERSION_IMPORTANCE, SCHEMA_SUBJECT_VERSION_DOC,
        SCHEMA_SUBJECT_VERSION_GROUP,
        SCHEMA_SUBJECT_VERSION_ORDER_IN_GROUP, SCHEMA_SUBJECT_VERSION_WIDTH, SCHEMA_SUBJECT_VERSION_DISPLAY);

  }

  static void addGCPConfig(ConfigDef configDef) {
    configDef.define(KEYFILE_CONFIG, KEYFILE_TYPE, KEYFILE_IMPORTANCE, KEYFILE_DOC);
    configDef.define(KEY_SOURCE_CONFIG, KEY_SOURCE_TYPE, KEY_SOURCE_DEFAULT, KEY_SOURCE_VALIDATOR, KEY_SOURCE_IMPORTANCE, KEY_SOURCE_DOC);
    configDef.define(PROJECT_CONFIG, PROJECT_TYPE, PROJECT_IMPORTANCE, PROJECT_DOC);
    configDef.define(PROJECT_LOCATION, PROJECT_LOCATION_TYPE, PROJECT_LOCATION_IMPORTANCE, PROJECT_LOCATION_DOC);
    configDef.define(GCS_BUCKET_NAME, GCS_BUCKET_NAME_TYPE, GCS_BUCKET_NAME_IMPORTANCE, GCS_BUCKET_NAME_DOC);
    configDef.define(GCS_ERROR_BUCKET_NAME, GCS_ERROR_BUCKET_NAME_TYPE, GCS_ERROR_BUCKET_NAME_DEFAULT, GCS_ERROR_BUCKET_NAME_IMPORTANCE,
        GCS_ERROR_BUCKET_NAME_DOC);
    configDef.define(GCS_ARCHIVE_BUCKET_NAME, GCS_ARCHIVE_BUCKET_NAME_TYPE, GCS_ARCHIVE_BUCKET_NAME_DEFAULT, GCS_ARCHIVE_BUCKET_NAME_IMPORTANCE,
        GCS_ARCHIVE_BUCKET_NAME_DOC);
    configDef.define(ERROR_BUCKET_CREATE_CONFIG, ERROR_BUCKET_CREATE_CONFIG_TYPE, ERROR_BUCKET_CREATE_CONFIG_DEFAULT,
        ERROR_BUCKET_CREATE_CONFIG_IMPORTANCE, ERROR_BUCKET_CREATE_CONFIG_DOC);
    configDef.define(ARCHIVE_BUCKET_CREATE_CONFIG, ARCHIVE_BUCKET_CREATE_CONFIG_TYPE, ARCHIVE_BUCKET_CREATE_CONFIG_DEFAULT,
        ARCHIVE_BUCKET_CREATE_CONFIG_IMPORTANCE, ARCHIVE_BUCKET_CREATE_CONFIG_DOC);

    configDef.define(PUB_SUB_TOPIC_NAME, PUB_SUB_TOPIC_NAME_TYPE, PUB_SUB_TOPIC_NAME_IMPORTANCE, PUB_SUB_TOPIC_NAME_DOC);
    configDef.define(PUB_SUB_DEAD_LETTER_TOPIC_NAME, PUB_SUB_DEAD_LETTER_TOPIC_NAME_TYPE, PUB_SUB_DEAD_LETTER_TOPIC_NAME_DEFAULT,
        PUB_SUB_DEAD_LETTER_TOPIC_NAME_IMPORTANCE, PUB_SUB_DEAD_LETTER_TOPIC_NAME_DOC);

    configDef.define(PUB_SUB_TOPIC_SUBSCRIPTION_NAME, PUB_SUB_TOPIC_SUBSCRIPTION_NAME_TYPE,
        PUB_SUB_TOPIC_SUBSCRIPTION_NAME_IMPORTANCE, PUB_SUB_TOPIC_SUBSCRIPTION_NAME_DOC);

    configDef.define(PUB_SUB_MESSAGE_ACK_DEADLINE, PUB_SUB_MESSAGE_ACK_DEADLINE_TYPE, PUB_SUB_MESSAGE_ACK_DEADLINE_DEFAULT
        , PUB_SUB_MESSAGE_ACK_DEADLINE_IMPORTANCE, PUB_SUB_MESSAGE_ACK_DEADLINE_DOC);

    configDef.define(PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME, PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_TYPE,
        PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_DEFAULT, PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_IMPORTANCE,
        PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME_DOC);

    configDef.define(BUCKET_CREATE_CONFIG, BUCKET_CREATE_TYPE, BUCKET_CREATE_DEFAULT, BUCKET_CREATE_IMPORTANCE, BUCKET_CREATE_DOC);
    configDef.define(BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG, BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_TYPE,
        BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_DEFAULT, BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_IMPORTANCE,
        BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG_DOC);
    configDef.define(PUB_SUB_TOPIC_CREATE_CONFIG, PUB_SUB_TOPIC_CREATE_TYPE, PUB_SUB_TOPIC_CREATE_DEFAULT, PUB_SUB_TOPIC_CREATE_IMPORTANCE,
        PUB_SUB_TOPIC_CREATE_DOC);
    configDef.define(PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE, PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_TYPE, PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_DEFAULT,
        PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_IMPORTANCE, PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE_DOC);
    configDef.define(PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_CONFIG, PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_TYPE, PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_DEFAULT,
        PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_IMPORTANCE, PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_DOC);
    configDef.define(GCS_RETRY_CONFIG, GCS_RETRY_TYPE, GCS_RETRY_DEFAULT, GCS_RETRY_VALIDATOR, GCS_RETRY_IMPORTANCE, GCS_RETRY_DOC);
    configDef.define(PUB_SUB_MESSAGE_HOLDING_DEADLINE, PUB_SUB_MESSAGE_HOLDING_DEADLINE_TYPE, PUB_SUB_MESSAGE_HOLDING_DEADLINE_DEFAULT,
        PUB_SUB_MESSAGE_HOLDING_DEADLINE_IMPORTANCE, PUB_SUB_MESSAGE_HOLDING_DEADLINE_DOC);


    configDef.define(PUB_SUB_BACK_OFF_TIME, PUB_SUB_BACK_OFF_TIME_TYPE, PUB_SUB_BACK_OFF_TIME_DEFAULT, PUB_SUB_BACK_OFF_TIME_VALIDATOR,
        PUB_SUB_BACK_OFF_TIME_IMPORTANCE, PUB_SUB_BACK_OFF_TIME_DOC);
    configDef.define(PUB_SUB_MAX_RETRY, PUB_SUB_MAX_RETRY_TYPE, PUB_SUB_MAX_RETRY_DEFAULT, PUB_SUB_MAX_RETRY_VALIDATOR,
        PUB_SUB_MAX_RETRY_IMPORTANCE, PUB_SUB_MAX_RETRY_DOC);
    configDef.define(STORAGE_RETRY_WAIT_CONFIG, STORAGE_RETRY_WAIT_CONFIG_TYPE, STORAGE_RETRY_WAIT_DEFAULT, STORAGE_RETRY_WAIT_VALIDATOR,
        STORAGE_RETRY_WAIT_IMPORTANCE, STORAGE_RETRY_WAIT_DOC);

    configDef.define(FILE_PROCESS_RETRY, FILE_PROCESS_RETRY_TYPE, FILE_PROCESS_RETRY_DEFAULT, FILE_PROCESS_RETRY_VALIDATOR,
        FILE_PROCESS_RETRY_IMPORTANCE, FILE_PROCESS_RETRY_DOC);

    configDef.define(SKIP_BAD_RECORDS, SKIP_BAD_RECORDS_TYPE, SKIP_BAD_RECORDS_DEFAULT, SKIP_BAD_RECORDS_IMPORTANCE, SKIP_BAD_RECORDS_DOC);

  }

  public static void validate(Map<String, String> props) {
    //TODO add more validations
  }
}
