package io.ray.streaming.runtime.worker.tasks;

import io.ray.streaming.operator.SourceOperator;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.core.processor.SourceProcessor;
import io.ray.streaming.runtime.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStreamTask extends StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);

  private final SourceProcessor sourceProcessor;

  /**
   * SourceStreamTask for executing a {@link SourceOperator}.
   * It is responsible for running the corresponding source operator.
   */
  public SourceStreamTask(int taskId, Processor sourceProcessor, JobWorker jobWorker) {
    super(taskId, sourceProcessor, jobWorker);
    this.sourceProcessor = (SourceProcessor) processor;
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    LOG.info("Source stream task thread start.");

    sourceProcessor.run();
  }

  @Override
  protected void cancelTask() {
  }

}
