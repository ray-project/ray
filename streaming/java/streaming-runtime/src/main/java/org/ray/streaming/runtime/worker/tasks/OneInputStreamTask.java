package org.ray.streaming.runtime.worker.tasks;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.worker.JobWorker;

public class OneInputStreamTask<IN> extends InputStreamTask {

  public OneInputStreamTask(int taskId, Processor processor, JobWorker streamWorker) {
    super(taskId, processor, streamWorker);
  }
}
