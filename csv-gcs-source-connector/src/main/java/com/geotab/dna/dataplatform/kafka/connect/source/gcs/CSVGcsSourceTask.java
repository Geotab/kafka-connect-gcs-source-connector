package com.geotab.dna.dataplatform.kafka.connect.source.gcs;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.BlobReaderConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.CSVBlobReaderConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.GeotabConnectMBean;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.GeotabJMXReporter;
import java.util.HashMap;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVGcsSourceTask extends PubSubGcsSourceTask {

  @Override
  public void start(Map<String, String> props) {
    super.start(props);
  }


  @Override
  protected BlobReaderConfig getBlobReaderConfig(Map<String, String> props) {
    return new CSVBlobReaderConfig(props);
  }

}
