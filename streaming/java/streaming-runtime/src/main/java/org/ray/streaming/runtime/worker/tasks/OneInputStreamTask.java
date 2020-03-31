package io.ray.streaming.runtime.worker.tasks;

import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.worker.JobWorker;

public class OneInputStreamTask<IN> extends InputStreamTask {

  public OneInputStreamTask(int taskId, Processor processor, JobWorker streamWorker) {
    super(taskId, processor, streamWorker);
  }
}
