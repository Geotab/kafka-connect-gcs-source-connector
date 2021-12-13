package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics.GeotabKafkaConnectMetric;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeotabConnectMBean implements DynamicMBean {

  private final ObjectName objectName;
  private Map<String, GeotabKafkaConnectMetric> metricMap;

  public GeotabConnectMBean(String mbeanName) throws MalformedObjectNameException {
    this.objectName = new ObjectName(mbeanName);
    this.metricMap = new ConcurrentHashMap<>();
  }

  public ObjectName name() {
    return this.objectName;
  }

  public void updateAttribute(String attributeName, GeotabKafkaConnectMetric measurable) {
    this.metricMap.put(attributeName, measurable);
  }


  @Override
  public Object getAttribute(String s) {
    if (!this.metricMap.containsKey(s)) {
      log.error("the metrics doesn't contain {}", s);
      return null;
    }
    return new Attribute(s, this.metricMap.get(s).measure());
  }

  @Override
  public void setAttribute(Attribute attribute) {
    throw new RuntimeException("not implemented, cannot be used");
  }

  @Override
  public AttributeList getAttributes(String[] strings) {
    AttributeList list = new AttributeList();
    for (String s : strings) {
      list.add(this.getAttribute(s));
    }

    return list;
  }

  /**
   * No need to implement, a map will be maintained to keep the updated metrics
   * @param attributeList the attribute list
   * @return the attribute list
   */
  @Override
  public AttributeList setAttributes(AttributeList attributeList) {
    throw new RuntimeException("not implemented, cannot be used");
  }

  @Override
  public Object invoke(String s, Object[] objects, String[] strings) {
    throw new RuntimeException("invoke method will not be used");
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    for (Map.Entry<String, GeotabKafkaConnectMetric> metric: this.metricMap.entrySet()) {
      if (metric.getValue().deletable()) {
        this.metricMap.remove(metric.getKey());
      }
    }

    MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[this.metricMap.size()];
    Iterator<String> attributes = this.metricMap.keySet().iterator();
    int index = 0;
    while (attributes.hasNext()) {
      attrs[index] = new MBeanAttributeInfo(attributes.next(), Integer.TYPE.getName(), "", true, false, false);
      index++;
    }
    return new MBeanInfo(this.getClass().getName(), "", attrs,
        (MBeanConstructorInfo[]) null, (MBeanOperationInfo[]) null, (MBeanNotificationInfo[]) null);
  }
}
