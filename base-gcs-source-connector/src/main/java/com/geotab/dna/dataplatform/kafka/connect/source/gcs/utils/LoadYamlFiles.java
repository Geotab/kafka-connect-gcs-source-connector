package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils;

import java.io.InputStream;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

@Slf4j
public class LoadYamlFiles {

  public synchronized static Map<String, Object> getProperties(String filename) {
    Yaml yaml = new Yaml();
    InputStream inputStream = LoadYamlFiles.class
        .getClassLoader().getResourceAsStream(filename);
    if (inputStream == null) {
      log.error("YAML file doesn't exist: {}", filename);
    }
    return yaml.load(inputStream);
  }

}
