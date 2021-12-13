package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics;

public interface Measure {
  void update(Object object);
  Object measure();
}
