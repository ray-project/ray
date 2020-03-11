package org.ray.streaming.runtime.worker.tasks;

import org.ray.runtime.util.Serializer;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.transfer.Message;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * The super class of one-input and two-input
 */
public abstract class InputStreamTask extends StreamTask {

  private long readTimeOutMillis;

  public InputStreamTask(int taskId, Processor processor, JobWorker jobWorker) {
    super(taskId, processor, jobWorker);
    readTimeOutMillis = jobWorker.getWorkerConfig().transferConfig.readerTimerIntervalMs();
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    while (running) {
      Message message = reader.read(readTimeOutMillis);
      if (message != null) {
        byte[] bytes = new byte[message.body().remaining()];
        message.body().get(bytes);
        Object obj = Serializer.decode(bytes);
        processor.process(obj);
      }
    }
  }

  @Override
  protected void cancelTask() throws Exception {
    running = false;
    while (!stopped) {
    }
  }
}
