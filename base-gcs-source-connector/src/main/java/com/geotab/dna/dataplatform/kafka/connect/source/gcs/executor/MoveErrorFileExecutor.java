package com.geotab.dna.dataplatform.kafka.connect.source.gcs.executor;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorTaskConfig.MOVE_ERROR_FILE_RATE;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.FileSystem;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner.BaseScanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class MoveErrorFileExecutor {
  private final int numOfMEFThread = 1;
  private final int EXECUTOR_SHUTDOWN_WAITING_TIME = 15;
  private ScheduledExecutorService scheduledThreadPool;
  private AbstractConfig config;

  public MoveErrorFileExecutor(AbstractConfig config) {
    this.scheduledThreadPool = Executors.newScheduledThreadPool(this.numOfMEFThread);
    this.config = config;
  }

  public void startMEFThread(BaseScanner deadLetterScanner, FileSystem fileSystem) {
    MoveErrorFileThread moveErrorFileThread = new MoveErrorFileThread(deadLetterScanner, fileSystem, config);
    this.scheduledThreadPool.scheduleAtFixedRate(moveErrorFileThread, config.getInt(MOVE_ERROR_FILE_RATE),
        config.getInt(MOVE_ERROR_FILE_RATE), TimeUnit.MINUTES);
  }

  public void stopMEFThread() {
    this.scheduledThreadPool.shutdown();
    try {
      boolean isShutDown = this.scheduledThreadPool.awaitTermination(EXECUTOR_SHUTDOWN_WAITING_TIME, TimeUnit.SECONDS);
      if (!isShutDown) {
        log.warn("move error file thread cannot shut down properly");
      }
    } catch (InterruptedException e) {
      log.warn("move error file thread is still running after {}s shutdown is signaled.", EXECUTOR_SHUTDOWN_WAITING_TIME);
    }

    log.info("executor has been shutdown");
  }
}