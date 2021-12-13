package com.geotab.dna.dataplatform.kafka.connect.source.gcs;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.cleanup.CleanUp;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.BlobReaderConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorTaskConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.executor.DefaultThread;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.executor.DefaultThreadPoolExecutor;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.executor.MoveErrorFileExecutor;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.FileSystem;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.GCSFileSystem;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.ProcessorReturnObject;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema.SchemaRetriever;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.collectmetrics.AbstractMetricProcess;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.collectmetrics.CommonMetricProcess;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics.Count;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.monitor.metrics.GeotabKafkaConnectMetric;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.reader.BlobReader;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner.BaseScanner;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner.DeadLetterSyncScanner;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

@Slf4j
@Setter
public class PubSubGcsSourceTask extends SourceTask {


  protected BaseScanner fileScanner;
  protected BaseScanner deadLetterScanner;
  protected PubSubGcsSourceConnectorTaskConfig config;
  protected BlobReaderConfig blobReaderConfig;
  protected CleanUp cleanUp;
  protected FileSystem fileSystem;
  protected List<FileMetaData> filesToCommit;
  private CredentialProvider credentialProvider;
  private DefaultThreadPoolExecutor threadPoolExecutor;
  private List<FileMetaData> filesFailed;
  private SchemaRetriever schemaRetriever;
  private MoveErrorFileExecutor moveErrorFileExecutor;
  private ConcurrentLinkedQueue<ProcessorReturnObject> returnObjects;
  protected AbstractMetricProcess metricProcess;

  @Override
  public String version() {
    return null;
  }

  public void initializeFileSystem() {
    this.fileSystem = new GCSFileSystem();
    this.fileSystem.initialize(this.config);
  }

  @Override
  public void start(Map<String, String> props) {
    Objects.requireNonNull(props, "props cannot be null");
    this.config = this.getTaskConfig(props);
    this.blobReaderConfig = this.getBlobReaderConfig(props);
    this.initializeFileSystem();
    //TODO if schema registry is not directly used here remove this
    this.schemaRetriever = this.config.getSchemaRetriever();
    this.credentialProvider = CredentialProvider.getCredentialProvider(this.config);
    this.fileScanner = this.config.getScanner();
    this.deadLetterScanner = new DeadLetterSyncScanner();
    this.deadLetterScanner.configure(props);
    fileScanner.startPoll();
    this.deadLetterScanner.startPoll();
    threadPoolExecutor = new DefaultThreadPoolExecutor(this.config.getInt(PubSubGcsSourceConnectorConfig.PROCESS_CORE_THREAD_POOL_SIZE)
        , this.config.getInt(PubSubGcsSourceConnectorConfig.PROCESS_MAX_THREAD_POOL_SIZE));
    this.cleanUp = this.config.getCleanUpStrategyClass();
    this.moveErrorFileExecutor = new MoveErrorFileExecutor(config);
    this.moveErrorFileExecutor.startMEFThread(deadLetterScanner, fileSystem);
    this.returnObjects = new ConcurrentLinkedQueue<>();
    this.filesToCommit = new LinkedList<>();
    this.initJMXMetrics();

  }

  protected void initJMXMetrics() {
    this.metricProcess = new CommonMetricProcess(this.config);
    this.metricProcess.registerMbean();
  }


  // adding this method to give more room for each blob reader customization
  protected BlobReader createBlobReader() {
    BlobReader blobReader = this.blobReaderConfig.getNewBlobReader();
    blobReader.setSchemaRetriever(schemaRetriever);
    return blobReader;
  }

  protected PubSubGcsSourceConnectorTaskConfig getTaskConfig(Map<String, String> props) {
    return new PubSubGcsSourceConnectorTaskConfig(props);
  }

  protected BlobReaderConfig getBlobReaderConfig(Map<String, String> props) {
    return new BlobReaderConfig(props);
  }

  @Override
  public List<SourceRecord> poll() {
    List<FileMetaData> filesToProcess = this.fileScanner.poll();
    //TODO change to returning Optional
    if (null != filesToProcess && !filesToProcess.isEmpty()) {
      log.info("number of files waiting to be processed: {} in thread {}", filesToProcess.size(), Thread.currentThread().getName());
      filesToProcess.forEach(fileMetaData -> {
        DefaultThread defaultThread = new DefaultThread(this.createBlobReader(), fileMetaData, this.returnObjects);
        this.threadPoolExecutor.execute(defaultThread);
        log.info("execute {} file", fileMetaData.getPath());
      });
    }
    if (returnObjects.isEmpty()) {
      return new LinkedList<>();
    }

    List<SourceRecord> sourceRecords = new LinkedList<>();
    int sizeOfReturnObjects = this.returnObjects.size();
    log.debug("got {} number of returned process objects ", sizeOfReturnObjects);

    while (sizeOfReturnObjects > 0) {
      ProcessorReturnObject returnObject = this.returnObjects.poll();

      sourceRecords.addAll(returnObject.getFileData());

      if (returnObject.getIsFailed()) {
        this.filesFailed.add(returnObject.getFileMetaData());
      } else {
        this.filesToCommit.add(returnObject.getFileMetaData());
      }

      this.setFileRecordsMetric(returnObject);
      this.metricProcess.collectMetrics(returnObject);

      sizeOfReturnObjects--;
    }
    log.info("gcs-pub-sub source-connector returning processed records with size {}", sourceRecords.size());
    return sourceRecords;
  }


  @Override
  public void stop() {
    if (null != this.fileScanner) {
      this.fileScanner.stopPoll();
    }

    if (null != this.deadLetterScanner) {
      this.deadLetterScanner.stopPoll();
    }

    if (null != this.threadPoolExecutor) {
      this.stopThreadPool();
    }

    if (null != this.moveErrorFileExecutor) {
      this.moveErrorFileExecutor.stopMEFThread();
    }

    this.metricProcess.unregisterMBean();

  }

  @Override
  public void commit() {
    List<FileMetaData> tempFilesToCommit = filesToCommit;
    filesToCommit = new LinkedList<>();
    List<FileMetaData> tempFilesFailed = filesFailed;
    filesFailed = new LinkedList<>();
    if (tempFilesToCommit != null && !tempFilesToCommit.isEmpty()) {
      tempFilesToCommit.forEach(fileMetaData -> {
        this.fileScanner.ackMessage(fileMetaData);
        this.cleanUp.cleanUpBlob(fileMetaData);

        //metric for num of acked files in each task
        this.metricProcess.incrementAckFileTotal();
      });
    }

    if (tempFilesFailed != null && !tempFilesFailed.isEmpty()) {
      tempFilesFailed.forEach(fileMetaData -> this.fileScanner.nackMessage(fileMetaData));
    }

  }

  public void stopThreadPool() {
    this.threadPoolExecutor.shutdown();
    int waitingTime = 15;
    try {
      this.threadPoolExecutor.awaitTermination(waitingTime, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.warn("{} active threads are still executing tasks {}s after shutdown is signaled.",
          threadPoolExecutor.getActiveCount(), waitingTime);
    }
    log.info("executor has been shutdown");
  }

  protected void setFileRecordsMetric(ProcessorReturnObject returnObject) {
    //metric for total num of records in each file
    GeotabKafkaConnectMetric fileTotalRecords = new Count("file_record_total", this.config);
    Map<String, Object> label = new HashMap<>();
    label.put("filename", returnObject.getFileMetaData().getPath());
    fileTotalRecords.setAttributeLabel(label);
    fileTotalRecords.update(returnObject.getFileData().size());
    log.info("attributeName:{}", fileTotalRecords.toAttributeName());
    this.metricProcess.updateMBeanAttribute(fileTotalRecords);
  }
}
