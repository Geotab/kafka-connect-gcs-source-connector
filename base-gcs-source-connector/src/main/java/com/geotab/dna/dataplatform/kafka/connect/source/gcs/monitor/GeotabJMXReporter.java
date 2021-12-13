package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor;

import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.JMException;
import javax.management.MBeanServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Sanitizer;

@Slf4j
public class GeotabJMXReporter {
  
  public static void register(GeotabConnectMBean mBean) {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    if (server.isRegistered(mBean.name())) {
      log.info("MBean {} has been registered", mBean.name());
      return;
    }

    try {
      server.registerMBean(mBean, mBean.name());
    } catch (JMException jmException) {
      throw new KafkaException("Error registering mBean " + mBean.name(), jmException);
    }
  }
  
  public static void unregister(GeotabConnectMBean mBean) {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      if (server.isRegistered(mBean.name())) {
        server.unregisterMBean(mBean.name());
      }

    } catch (JMException jmException) {
      log.error("Error unregistering mbean {}", mBean.name(), jmException);
    }
  }

  /**
   *
   * @param domain the MBean domain name
   * @param group either sink or source
   * @param beanProperties all related MBean properties
   */
  public static String toMBeanName(String domain, String group, Map<String, Object> beanProperties) {
    StringBuilder stringBuilder = new StringBuilder(domain);
    stringBuilder.append(":type=");
    stringBuilder.append(group);
    for (Map.Entry<String, Object> beanProperty: beanProperties.entrySet()) {
      stringBuilder.append(",");
      stringBuilder.append(beanProperty.getKey());
      stringBuilder.append("=");
      stringBuilder.append(beanProperty.getValue());
    }
    return stringBuilder.toString();
  }

}
