package io.ray.streaming.runtime.worker.tasks;

import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.core.processor.TwoInputProcessor;
import io.ray.streaming.runtime.worker.JobWorker;

/** Input stream task with 2 inputs. Such as: join operator. */
public class TwoInputStreamTask extends InputStreamTask {

  public TwoInputStreamTask(
      Processor processor,
      JobWorker jobWorker,
      String leftStream,
      String rightStream,
      long lastCheckpointId) {
    super(processor, jobWorker, lastCheckpointId);
    ((TwoInputProcessor) (super.processor)).setLeftStream(leftStream);
    ((TwoInputProcessor) (super.processor)).setRightStream(rightStream);
  }
}
