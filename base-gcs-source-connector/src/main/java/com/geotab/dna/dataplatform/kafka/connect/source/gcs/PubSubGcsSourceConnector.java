package com.geotab.dna.dataplatform.kafka.connect.source.gcs;


import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.Version;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.GcpResourcesUtil;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.LoadYamlFiles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

@Slf4j
public class PubSubGcsSourceConnector extends SourceConnector {
  protected Map<String, String> configProperties;
  protected GcpResourcesUtil gcpResourcesUtil;
  PubSubGcsSourceConnectorConfig pubSubGcsSourceConnectorConfig;

  @Override
  public void start(Map<String, String> props) {
    final String connectName = props.get(PubSubGcsSourceConnectorConfig.getConnectorNameConfig());
    log.info("Configuring connector : {}", connectName);

    Map<String, String> defaultProps = loadDefaultProperties("properties.yaml");

    if (!defaultProps.isEmpty()) {
      for (Map.Entry<String, String> entry : defaultProps.entrySet()) {
        if (!props.containsKey(entry.getKey())) {
          props.put(entry.getKey(), entry.getValue());
        }
      }
    }

    try {
      this.configProperties = Collections.unmodifiableMap(props);
      this.pubSubGcsSourceConnectorConfig = new PubSubGcsSourceConnectorConfig(props);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't init PUBSUB GCS source connector due to configuration error", e);
    }

    this.gcpResourcesUtil = new GcpResourcesUtil(pubSubGcsSourceConnectorConfig);
    this.gcpResourcesUtil.createGcpResources();

  }


  public Map<String, String> loadDefaultProperties(String fileName) {
    Map<String, Object> loadedProperties = LoadYamlFiles.getProperties(fileName);
    return loadedProperties.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
  }


  @Override
  public Class<? extends Task> taskClass() {
    return PubSubGcsSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {

    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      final Map<String, String> config = new HashMap<>(configProperties);
      configs.add(config);
    }
    return configs;
    /*
    TODO
    take max tasks parameter and return no of list of maps that will be used to initialize tasks. it will have the same no of tasks as the number of
    elements in list we return. we can have same set of parameters to all the tasks or we can change the set of params returned to them
     */
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return PubSubGcsSourceConnectorConfig.configDef();
  }

  @Override
  public String version() {
    return Version.VERSION;
  }

}
