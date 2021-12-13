package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class AsyncPubSubConfig extends PubSubConfig {
  public static final String ASYNC_PULL_THREAD_POOL = "asyncThreadPool";
  public static final String ASYNC_PULL_PARALLEL_PULL = "asyncParallelPull";
  public static final String ASYNC_PULL_FLOW_CONTROL = "asyncFlowControlLimit";
  private static final ConfigDef.Type ASYNC_PULL_THREAD_POOL_TYPE = ConfigDef.Type.INT;
  private static final Integer ASYNC_PULL_THREAD_POOL_DEFAULT = 2;
  private static final ConfigDef.Importance ASYNC_PULL_THREAD_POOL_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final ConfigDef.Validator ASYNC_PULL_THREAD_POOL_VALIDATOR = ConfigDef.Range.between(1, 10);
  private static final String ASYNC_PULL_THREAD_POOL_DOC = "no of threads to be launched to read data from pubsub using pubsub concurrent model";
  private static final ConfigDef.Type ASYNC_PULL_PARALLEL_PULL_TYPE = ConfigDef.Type.INT;
  private static final Integer ASYNC_PULL_PARALLEL_PULL_DEFAULT = 2;
  private static final ConfigDef.Importance ASYNC_PULL_PARALLEL_PULL_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final ConfigDef.Validator ASYNC_PULL_PARALLEL_PULL_VALIDATOR = ConfigDef.Range.between(1, 10);

  private static final String ASYNC_PULL_PARALLEL_PULL_DOC = "Parallel pull count for Async pubsub concurrent model";
  private static final ConfigDef.Type ASYNC_PULL_FLOW_CONTROL_TYPE = ConfigDef.Type.INT;
  private static final Integer ASYNC_PULL_FLOW_CONTROL_DEFAULT = 2;
  private static final ConfigDef.Importance ASYNC_PULL_FLOW_CONTROL_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final ConfigDef.Validator ASYNC_PULL_FLOW_CONTROL_VALIDATOR = ConfigDef.Range.between(1, 10);
  private static final String ASYNC_PULL_FLOW_CONTROL_DOC = "Max no of messages that can be outstanding at a subscriber";

  public AsyncPubSubConfig(ConfigDef configDef, Map<String, String> originalProperties) {
    super(configDef, originalProperties);
  }

  public AsyncPubSubConfig(Map<String, String> originalProperties) {
    this(addFileProcessConfig(), originalProperties);
  }

  static ConfigDef addFileProcessConfig() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(ASYNC_PULL_PARALLEL_PULL, ASYNC_PULL_PARALLEL_PULL_TYPE, ASYNC_PULL_PARALLEL_PULL_DEFAULT, ASYNC_PULL_PARALLEL_PULL_VALIDATOR,
        ASYNC_PULL_PARALLEL_PULL_IMPORTANCE, ASYNC_PULL_PARALLEL_PULL_DOC);
    configDef.define(ASYNC_PULL_FLOW_CONTROL, ASYNC_PULL_FLOW_CONTROL_TYPE, ASYNC_PULL_FLOW_CONTROL_DEFAULT, ASYNC_PULL_FLOW_CONTROL_VALIDATOR,
        ASYNC_PULL_FLOW_CONTROL_IMPORTANCE, ASYNC_PULL_FLOW_CONTROL_DOC);
    configDef.define(ASYNC_PULL_THREAD_POOL, ASYNC_PULL_THREAD_POOL_TYPE, ASYNC_PULL_THREAD_POOL_DEFAULT, ASYNC_PULL_THREAD_POOL_VALIDATOR,
        ASYNC_PULL_THREAD_POOL_IMPORTANCE, ASYNC_PULL_THREAD_POOL_DOC);
    return configDef;
  }

}
