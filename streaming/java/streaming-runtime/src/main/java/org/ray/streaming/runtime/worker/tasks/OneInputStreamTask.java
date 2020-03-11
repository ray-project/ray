package org.ray.streaming.runtime.worker.tasks;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.worker.JobWorker;

public class OneInputStreamTask extends InputStreamTask {

  public OneInputStreamTask(int taskId, Processor inputProcessor, JobWorker jobWorker) {
    super(taskId, inputProcessor, jobWorker);
  }

}
