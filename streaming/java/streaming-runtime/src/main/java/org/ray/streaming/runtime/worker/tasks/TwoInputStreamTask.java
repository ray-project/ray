package org.ray.streaming.runtime.worker.tasks;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.processor.TwoInputProcessor;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Two input type. e.g. join
 */
public class TwoInputStreamTask extends InputStreamTask {

  public TwoInputStreamTask(int taskId, Processor processor, JobWorker jobWorker,
      String leftStream, String rightStream) {
    super(taskId, processor, jobWorker);
    ((TwoInputProcessor)(super.processor)).setLeftStream(leftStream);
    ((TwoInputProcessor)(super.processor)).setRightStream(rightStream);
  }
}
