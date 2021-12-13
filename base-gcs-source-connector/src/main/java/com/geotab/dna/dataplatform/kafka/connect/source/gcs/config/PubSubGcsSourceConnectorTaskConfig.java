package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup.CleanUp;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CleanUpStrategy;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema.SchemaRetriever;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.reader.BlobReader;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner.BaseScanner;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

@Slf4j
public class PubSubGcsSourceConnectorTaskConfig extends PubSubGcsSourceConnectorConfig {

  public static final String BLOB_READER = "blobReaderClass";
  public static final String FILE_SCANNER = "fileScannerClass";
  public static final String SCHEMA_RETRIEVER_CONFIG = "schemaRetriever";
  public static final String MOVE_ERROR_FILE_RATE = "moveErrorFileRate";
  public static final String MOVE_ERROR_FILES = "moveErrorFiles";
  public static final boolean MOVE_ERROR_FILES_DEFAULT = true;
  static final String FILE_SCANNER_DOC = "Class name of the file scanner";
  private static final ConfigDef.Type BLOB_READER_TYPE = ConfigDef.Type.CLASS;
  private static final ConfigDef.Importance BLOB_READER_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String BLOB_READER_DOC = "Class name of the blob reader that needs to process blob";
  private static final Class<?> BLOB_READER_DEFAULT = BlobReader.class;
  private static final ConfigDef.Type FILE_SCANNER_TYPE = ConfigDef.Type.CLASS;
  private static final ConfigDef.Importance FILE_SCANNER_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final Class<?> FILE_SCANNER_DEFAULT = BaseScanner.class;
  private static final ConfigDef.Type SCHEMA_RETRIEVER_TYPE = ConfigDef.Type.CLASS;
  private static final Class<?> SCHEMA_RETRIEVER_DEFAULT = null;
  private static final ConfigDef.Importance SCHEMA_RETRIEVER_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final ConfigDef.Type MOVE_ERROR_FILE_RATE_TYPE = ConfigDef.Type.INT;
  private static final ConfigDef.Importance MOVE_ERROR_FILE_RATE_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final Integer MOVE_ERROR_FILE_RATE_DEFAULT = 1;
  private static final String MOVE_ERROR_FILE_RATE_DOC = "how often the deadletter pubsub queue is pull";
  private static final String SCHEMA_RETRIEVER_DOC = "A class that can be used for automatically creating tables and/or updating schemas";
  private static final ConfigDef.Type MOVE_ERROR_FILES_TYPE = ConfigDef.Type.BOOLEAN;
  private static final ConfigDef.Importance MOVE_ERROR_FILES_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String MOVE_ERROR_FILES_DOC = "flag to decide weather to move error files or not";
  private static String BLOB_READER_PREFIX = "blobReader.";
  public static final String SOURCE_CONNECT_METRICS_EXPIRATION_TIME = "staleTimeSeconds";
  private static final ConfigDef.Type SOURCE_CONNECT_METRICS_EXPIRATION_TIME_TYPE = ConfigDef.Type.LONG;
  public static final Long SOURCE_CONNECT_METRICS_EXPIRATION_TIME_DEFAULT = 3600L;
  public static final String SOURCE_CONNECT_METRICS_EXPIRATION_TIME_DOC = "how long will the metrics be cleaned up when metrics have no update";
  private static final ConfigDef.Importance SOURCE_CONNECT_METRICS_EXPIRATION_TIME_IMPORTANCE = ConfigDef.Importance.MEDIUM;

  public PubSubGcsSourceConnectorTaskConfig(Map<String, String> originalProperties) {
    super(configDef(new ConfigDef()), originalProperties);

  }

  public PubSubGcsSourceConnectorTaskConfig(ConfigDef configDef, Map<String, String> properties) {
    super(configDef(configDef), properties);
  }

  public static ConfigDef configDef(ConfigDef configDef) {
    addTaskConfigs(configDef);
    return configDef;
  }


  static void addTaskConfigs(ConfigDef configDef) {
    configDef.define(BLOB_READER, BLOB_READER_TYPE, BLOB_READER_DEFAULT, BLOB_READER_IMPORTANCE, BLOB_READER_DOC);
    configDef.define(FILE_SCANNER, FILE_SCANNER_TYPE, FILE_SCANNER_DEFAULT, FILE_SCANNER_IMPORTANCE, FILE_SCANNER_DOC);
    configDef.define(SCHEMA_RETRIEVER_CONFIG, SCHEMA_RETRIEVER_TYPE, SCHEMA_RETRIEVER_DEFAULT, SCHEMA_RETRIEVER_IMPORTANCE, SCHEMA_RETRIEVER_DOC);
    configDef.define(MOVE_ERROR_FILE_RATE, MOVE_ERROR_FILE_RATE_TYPE, MOVE_ERROR_FILE_RATE_DEFAULT,
        MOVE_ERROR_FILE_RATE_IMPORTANCE, MOVE_ERROR_FILE_RATE_DOC);
    configDef.define(MOVE_ERROR_FILES, MOVE_ERROR_FILES_TYPE, MOVE_ERROR_FILES_DEFAULT, MOVE_ERROR_FILES_IMPORTANCE, MOVE_ERROR_FILES_DOC);
    configDef
        .define(SOURCE_CONNECT_METRICS_EXPIRATION_TIME, SOURCE_CONNECT_METRICS_EXPIRATION_TIME_TYPE, SOURCE_CONNECT_METRICS_EXPIRATION_TIME_DEFAULT,
            SOURCE_CONNECT_METRICS_EXPIRATION_TIME_IMPORTANCE, SOURCE_CONNECT_METRICS_EXPIRATION_TIME_DOC);
  }

  public static void validate(Map<String, String> props) {
    //TODO add more validations
    log.info("will check later");
  }

  public boolean moveErrorFiles() {
    return this.getBoolean(MOVE_ERROR_FILES);
  }

  public SchemaRetriever getSchemaRetriever() {
    Class<?> userSpecifiedClass = getClass(SCHEMA_RETRIEVER_CONFIG);

    if (userSpecifiedClass == null) {
      throw new ConfigException(
          "Cannot request new instance of SchemaRetriever when class has not been specified"
      );
    }

    if (!SchemaRetriever.class.isAssignableFrom(userSpecifiedClass)) {
      throw new ConfigException(
          "Class specified for " + SCHEMA_RETRIEVER_CONFIG
              + " property does not implement " + SchemaRetriever.class.getName()
              + " interface"
      );
    }

    Class<? extends SchemaRetriever> schemaRetrieverClass =
        userSpecifiedClass.asSubclass(SchemaRetriever.class);

    Constructor<? extends SchemaRetriever> schemaRetrieverConstructor = null;
    try {
      schemaRetrieverConstructor = schemaRetrieverClass.getConstructor();
    } catch (NoSuchMethodException nsme) {
      throw new ConfigException(
          "Class specified for SchemaRetriever must have a no-args constructor",
          nsme
      );
    }

    SchemaRetriever schemaRetriever = null;
    try {
      schemaRetriever = schemaRetrieverConstructor.newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        exception) {
      throw new ConfigException(
          "Failed to instantiate class specified for SchemaRetriever",
          exception
      );
    }

    schemaRetriever.configure(originalsStrings());

    return schemaRetriever;
  }

  public BaseScanner getScanner() {
    Class<?> userSpecifiedClass = getClass(FILE_SCANNER);
    if (userSpecifiedClass == null) {
      throw new ConfigException(
          "Cannot request new instance of File Scanner when class has not been specified"
      );
    }
    Class<? extends BaseScanner> baseScannerClass = userSpecifiedClass.asSubclass(BaseScanner.class);

    if (!BaseScanner.class.isAssignableFrom(userSpecifiedClass)) {
      throw new ConfigException(
          String.format("Class specified for %s property does not implement %s interface", BLOB_READER, BaseScanner.class.getName()));
    }
    Constructor<? extends BaseScanner> baseScannerConstructor = null;
    try {
      baseScannerConstructor = baseScannerClass.getConstructor();
    } catch (NoSuchMethodException e) {
      throw new ConfigException("Class specified for SchemaRetriever must have a no-args constructor", e);
    }

    BaseScanner baseScanner = null;
    try {
      log.info("class name specified is {}", userSpecifiedClass.getName());
      baseScanner = baseScannerConstructor.newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigException("Failed to instantiate class specified for Blob Scanner", e);
    }
    baseScanner.configure(originalsStrings());
    return baseScanner;
  }

  public CleanUp getCleanUpStrategyClass() {

    Class<?> userSpecifiedClass = CleanUpStrategy.valueOf(this.getString(CLEAN_UP_STRATEGY)).getCleanUpStrategyClass();
    if (userSpecifiedClass == null) {
      throw new ConfigException(
          "Cannot request new instance of Blob Reader when class has not been specified"
      );
    }
    Class<? extends CleanUp> cleanUpStrategyClass = userSpecifiedClass.asSubclass(CleanUp.class);

    if (!CleanUp.class.isAssignableFrom(userSpecifiedClass)) {
      throw new ConfigException(
          String.format("Class specified for %s property does not implement %s interface", BLOB_READER, BlobReader.class.getName()));
    }
    Constructor<? extends CleanUp> cleanUpStrategyClassConstructor = null;
    try {
      cleanUpStrategyClassConstructor = cleanUpStrategyClass.getConstructor();
    } catch (NoSuchMethodException e) {
      throw new ConfigException("Class specified for SchemaRetriever must have a no-args constructor", e);
    }

    CleanUp cleanUp = null;
    try {
      cleanUp = cleanUpStrategyClassConstructor.newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigException("Failed to instantiate class specified for SchemaRetriever", e);
    }
    cleanUp.configure(this);
    return cleanUp;
  }

  public BlobReader getNewBlobReader() {

    Class<?> userSpecifiedClass = getClass(BLOB_READER);
    if (userSpecifiedClass == null) {
      throw new ConfigException(
          "Cannot request new instance of Blob Reader when class has not been specified"
      );
    }
    Class<? extends BlobReader> blobReaderClass = userSpecifiedClass.asSubclass(BlobReader.class);

    if (!BlobReader.class.isAssignableFrom(userSpecifiedClass)) {
      throw new ConfigException(
          String.format("Class specified for %s property does not implement %s interface", BLOB_READER, BlobReader.class.getName()));
    }
    Constructor<? extends BlobReader> blobReaderConstructor = null;
    try {
      blobReaderConstructor = blobReaderClass.getConstructor();
    } catch (NoSuchMethodException e) {
      throw new ConfigException("Class specified for SchemaRetriever must have a no-args constructor", e);
    }

    BlobReader blobReader = null;
    try {
      blobReader = blobReaderConstructor.newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigException("Failed to instantiate class specified for SchemaRetriever", e);
    }
    blobReader.configure(this);
    return blobReader;
  }
}
