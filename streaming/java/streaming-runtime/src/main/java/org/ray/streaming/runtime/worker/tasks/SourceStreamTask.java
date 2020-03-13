package org.ray.streaming.runtime.worker.tasks;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SourceStreamTask for executing a {@link org.ray.streaming.operator.impl.SourceOperator}.
 * It is responsible for running the corresponding source operator.
 */
public class SourceStreamTask extends StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(
      SourceStreamTask.class);

  private final SourceProcessor sourceProcessor;

  public SourceStreamTask(int taskId, Processor sourceProcessor, JobWorker jobWorker) {
    super(taskId, sourceProcessor, jobWorker);
    this.sourceProcessor = (SourceProcessor) processor;
  }

  @Override
  protected void init() throws Exception {
  }

  @Override
  protected void cancelTask() throws Exception {
  }

  @Override
  public void run() {
    LOG.info("Source stream task thread start.");

    while (running) {
      sourceProcessor.run();
    }
  }
}
