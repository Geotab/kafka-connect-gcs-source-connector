package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class Count extends GeotabKafkaConnectMetric {

  private Long total;

  public Count(String metricName, AbstractConfig config) {
    super(metricName, config);
    this.total = 0L;
  }

  public Count(String metricName, long total, AbstractConfig config) {
    super(metricName, config);
    this.total = total;
  }

  @Override
  public void update(Object object) {
    super.update(object);
    this.total += ((Number) object).longValue();
  }

  @Override
  public Object measure() {
    return this.total;
  }

}
