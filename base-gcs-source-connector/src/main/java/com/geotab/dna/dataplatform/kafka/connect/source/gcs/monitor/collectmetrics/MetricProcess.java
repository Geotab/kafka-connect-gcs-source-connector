package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.collectmetrics;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.ProcessorReturnObject;

public interface MetricProcess {
  void collectMetrics(ProcessorReturnObject returnObject);
}
