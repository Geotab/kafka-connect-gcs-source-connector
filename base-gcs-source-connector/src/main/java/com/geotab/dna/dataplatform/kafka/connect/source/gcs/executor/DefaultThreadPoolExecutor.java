package com.geotab.dna.dataplatform.kafka.connect.source.gcs.executor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultThreadPoolExecutor extends ThreadPoolExecutor {

  public DefaultThreadPoolExecutor(int corePoolSize, int maxPoolSize) {
    super(corePoolSize, maxPoolSize, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
  }

  @Override
  protected void afterExecute(Runnable runnable, Throwable throwable) {
    super.afterExecute(runnable, throwable);

    // do nothing for now
  }

}
