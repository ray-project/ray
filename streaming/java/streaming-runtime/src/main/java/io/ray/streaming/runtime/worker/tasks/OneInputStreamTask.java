package io.ray.streaming.runtime.worker.tasks;

import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.worker.JobWorker;

/**
 * Input stream task with 1 input. Such as: map operator.
 */
public class OneInputStreamTask extends InputStreamTask {

  public OneInputStreamTask(int taskId, Processor inputProcessor, JobWorker jobWorker) {
    super(taskId, inputProcessor, jobWorker);
  }
}
