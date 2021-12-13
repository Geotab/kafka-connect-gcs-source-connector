package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorTaskConfig.SOURCE_CONNECT_METRICS_EXPIRATION_TIME;


import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorTaskConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics.Measure;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.apache.kafka.common.config.AbstractConfig;

@Data
public abstract class GeotabKafkaConnectMetric implements Measure {

  private final String metricName;
  protected Map<String, Object> attributeLabel;
  protected Long epochTimestamp;
  protected Long staleTimeSeconds;

  public GeotabKafkaConnectMetric(String metricName, AbstractConfig config) {
    PubSubGcsSourceConnectorTaskConfig pubsSubTaskConfig = (PubSubGcsSourceConnectorTaskConfig) config;
    this.staleTimeSeconds = pubsSubTaskConfig.getLong(SOURCE_CONNECT_METRICS_EXPIRATION_TIME);
    this.metricName = metricName;
    this.attributeLabel = new HashMap<>();
    this.epochTimestamp = Instant.now().getEpochSecond();
  }

  public String toAttributeName() {
    if (this.attributeLabel.isEmpty()) {
      return this.metricName;
    }

    StringBuilder stringBuilder = new StringBuilder(this.metricName);
    stringBuilder.append(":");
    for (Map.Entry<String, Object> entry : attributeLabel.entrySet()) {
      stringBuilder.append(entry.getKey());
      stringBuilder.append("=");
      stringBuilder.append(entry.getValue());
      stringBuilder.append(",");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    return stringBuilder.toString();
  }

  // metric becomes deletable if the last update is 1 hour ago by default
  public boolean deletable() {
    return Instant.now().getEpochSecond() - this.epochTimestamp > this.staleTimeSeconds;
  }

  @Override
  public void update(Object object) {
    this.epochTimestamp = Instant.now().getEpochSecond();
  }
}
