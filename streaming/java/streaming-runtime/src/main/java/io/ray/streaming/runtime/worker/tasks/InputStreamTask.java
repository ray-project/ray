package io.ray.streaming.runtime.worker.tasks;

import com.google.common.base.MoreObjects;
import io.ray.runtime.serializer.Serializer;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.transfer.Message;
import io.ray.streaming.runtime.worker.JobWorker;

public abstract class InputStreamTask extends StreamTask {

  private long readTimeoutMillis;

  public InputStreamTask(int taskId, Processor processor, JobWorker jobWorker) {
    super(taskId, processor, jobWorker);
    readTimeoutMillis = jobWorker.getWorkerConfig().transferConfig.readerTimerIntervalMs();
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    while (running) {
      Message message = reader.read(readTimeoutMillis);
      if (message != null) {
        byte[] bytes = new byte[message.body().remaining()];
        message.body().get(bytes);
        Object obj = Serializer.decode(bytes, Object.class);
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("taskId", taskId)
      .add("processor", processor)
      .toString();
  }
}
