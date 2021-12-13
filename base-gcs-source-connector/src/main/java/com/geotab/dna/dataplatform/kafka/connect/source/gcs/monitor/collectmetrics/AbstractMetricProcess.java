package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.collectmetrics;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.GeotabConnectMBean;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.GeotabJMXReporter;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics.Count;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics.GeotabKafkaConnectMetric;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.LoadYamlFiles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.MDC;

@Slf4j
@Data
public abstract class AbstractMetricProcess implements MetricProcess {
  protected GeotabConnectMBean mBean;
  protected Map<String, GeotabKafkaConnectMetric> metricMap;
  protected Map<String, Object> connectorMetricsProps;

  public AbstractMetricProcess(AbstractConfig config) {
    this.connectorMetricsProps = LoadYamlFiles.getProperties("mbean-metrics.yaml");
    List<String> metricsName = (List<String>) connectorMetricsProps.get("metrics");

    this.metricMap = new HashMap<>();
    for (String eachName : metricsName) {
      this.metricMap.put(eachName, new Count(eachName, config));
    }
  }

  public void registerMbean() {
    String[] connectContext = MDC.get("connector.context").replaceAll("[\\[\\](){}]", "").split("\\|");
    Map<String, Object> beanProperties = new HashMap<>();
    beanProperties.put("application", connectContext[0]);
    beanProperties.put("taskId", connectContext[1]);
    String mBeanName = GeotabJMXReporter
        .toMBeanName(String.valueOf(connectorMetricsProps.get("mbeanDomainName")), String.valueOf(connectorMetricsProps.get("sourceGroup")),
            beanProperties);
    try {
      this.mBean = new GeotabConnectMBean(mBeanName);
    } catch (MalformedObjectNameException e) {
      log.error("mBean name is not correct", e);
    }

    GeotabJMXReporter.register(this.mBean);
  }

  public void unregisterMBean() {
    if (null != this.mBean) {
      GeotabJMXReporter.unregister(this.mBean);
    }
  }

  public void updateMetric(String metricName, Object value) {
    if (!this.metricMap.containsKey(metricName)) {
      log.error("cannot find out the metric {}, ignore the metrics value", metricName);
      return;
    }

    GeotabKafkaConnectMetric metric = this.metricMap.get(metricName);
    metric.update(value);
    this.metricMap.put(metricName, metric);
  }

  /**
   * this method is used for the accumulative metrics
   * @param metricName the metrics name
   */
  public void updateMBeanAttribute(String metricName) {
    if (!this.metricMap.containsKey(metricName)) {
      log.error("cannot find out the metric {}. ignore the mbean updating", metricName);
      return;
    }

    GeotabKafkaConnectMetric metric = this.metricMap.get(metricName);
    this.mBean.updateAttribute(metric.toAttributeName(), metric);

  }

  /**
   * this method is used for metrics which is not accumulative, like number of records per file
   * @param metric the metric object
   */
  public void updateMBeanAttribute(GeotabKafkaConnectMetric metric) {
    this.mBean.updateAttribute(metric.toAttributeName(), metric);
  }

  public void incrementAckFileTotal() {
    this.updateMetric("pubsub_acked_file_total", 1);
    this.updateMBeanAttribute("pubsub_acked_file_total");
  }
}
