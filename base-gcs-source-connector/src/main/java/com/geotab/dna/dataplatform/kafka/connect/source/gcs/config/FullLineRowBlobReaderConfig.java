package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class FullLineRowBlobReaderConfig extends BlobReaderConfig {
  public FullLineRowBlobReaderConfig(ConfigDef configDef, Map<String, String> originalProperties) {
    super(configDef, originalProperties);
  }

  public FullLineRowBlobReaderConfig(Map<String, String> originalProperties) {
    this(addFileProcessConfig(), originalProperties);
  }

  static ConfigDef addFileProcessConfig() {
    return new ConfigDef();
  }

}
