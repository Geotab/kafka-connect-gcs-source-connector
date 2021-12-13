package com.geotab.dna.dataplatform.kafka.connect.source.gcs;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.Task;

@Slf4j
public class CSVGcsSourceConnector extends PubSubGcsSourceConnector {

  @Override
  public void start(Map<String, String> props) {
    super.start(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CSVGcsSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return super.taskConfigs(maxTasks);
  }
}
