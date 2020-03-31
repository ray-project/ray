package io.ray.streaming.runtime.worker.tasks;

import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.core.processor.SourceProcessor;
import io.ray.streaming.runtime.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStreamTask<IN> extends StreamTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SourceStreamTask.class);

  public SourceStreamTask(int taskId, Processor processor, JobWorker worker) {
    super(taskId, processor, worker);
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    final SourceProcessor<IN> sourceProcessor = (SourceProcessor<IN>) this.processor;
    sourceProcessor.run();
  }

  @Override
  protected void cancelTask() throws Exception {
  }

}
