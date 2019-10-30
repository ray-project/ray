package org.ray.streaming.runtime.tasks;

import org.ray.streaming.core.processor.Processor;
import org.ray.streaming.runtime.JobWorker;

public class OneInputStreamTask<IN> extends InputStreamTask {

  public OneInputStreamTask(int taskId, Processor processor, JobWorker streamWorker) {
    super(taskId, processor, streamWorker);
  }
}
