package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model;


import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {
  public static final String VERSION;
  private static final Logger log = LoggerFactory.getLogger(Version.class);
  private static final String PROPERTIES_FILENAME = "geotab-kafka-connect-gcs-version.properties";

  static {
    final Properties props = new Properties();
    try (final InputStream resourceStream =
             Version.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME)) {
      props.load(resourceStream);
    } catch (final Exception e) {
      log.warn("Error while loading {}: {}", PROPERTIES_FILENAME, e.getMessage());
    }
    VERSION = props.getProperty("version", "unknown").trim();
  }
}