package com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.collectmetrics;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.ProcessorReturnObject;
import org.apache.kafka.common.config.AbstractConfig;

public class CommonMetricProcess extends AbstractMetricProcess {

  public CommonMetricProcess(AbstractConfig config) {
    super(config);
  }

  @Override
  public void collectMetrics(ProcessorReturnObject returnObject) {
    if (returnObject.getIsFailed()) {
      this.incrementFailedFileNum();
    } else if (returnObject.getIsSKipped()) {
      this.incrementSkipFileNum();
    } else {
      this.incrementSuccessFileNum();
    }

    this.cumulateTotalRecords(returnObject.getFileData().size());
  }

  private void incrementSuccessFileNum() {
    this.updateMetric("file_process_success_total", 1);
    this.updateMBeanAttribute("file_process_success_total");
  }

  private void incrementFailedFileNum() {
    this.updateMetric("file_process_failure_total", 1);
    this.updateMBeanAttribute("file_process_failure_total");
  }

  private void incrementSkipFileNum() {
    this.updateMetric("file_process_skip_total", 1);
    this.updateMBeanAttribute("file_process_skip_total");
  }

  private void cumulateTotalRecords(Object numRecords) {
    this.updateMetric("record_total", numRecords);
    this.updateMBeanAttribute("record_total");
  }

}
