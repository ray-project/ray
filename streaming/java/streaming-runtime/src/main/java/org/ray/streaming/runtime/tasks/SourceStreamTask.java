package org.ray.streaming.runtime.tasks;

import org.ray.streaming.core.processor.Processor;
import org.ray.streaming.core.processor.SourceProcessor;
import org.ray.streaming.runtime.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStreamTask<IN> extends StreamTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SourceStreamTask.class);

  private volatile boolean running = true;
  private volatile Long batchId = null;

  public SourceStreamTask(int taskId, Processor processor, JobWorker worker) {
    super(taskId, processor, worker);
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    final SourceProcessor<IN> sourceProcessor = (SourceProcessor<IN>) this.processor;
    while (batchId == null) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOGGER.error("Wait batchId interrupted", e);
        throw new RuntimeException(e);
      }
    }
    while (running) {
      sourceProcessor.process(batchId);
    }
  }

  @Override
  protected void cancelTask() throws Exception {
    running = false;
  }

  public void setBatchId(Long batchId) {
    this.batchId = batchId;
  }

}
