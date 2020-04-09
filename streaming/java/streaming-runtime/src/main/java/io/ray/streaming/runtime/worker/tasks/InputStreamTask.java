package io.ray.streaming.runtime.worker.tasks;

import io.ray.runtime.serializer.Serializer;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.transfer.Message;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.util.Config;

public abstract class InputStreamTask extends StreamTask {
  private volatile boolean running = true;
  private volatile boolean stopped = false;
  private long readTimeoutMillis;

  public InputStreamTask(int taskId, Processor processor, JobWorker streamWorker) {
    super(taskId, processor, streamWorker);
    readTimeoutMillis = Long.parseLong((String) streamWorker.getConfig()
        .getOrDefault(Config.READ_TIMEOUT_MS, Config.DEFAULT_READ_TIMEOUT_MS));
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    while (running) {
      Message item = reader.read(readTimeoutMillis);
      if (item != null) {
        byte[] bytes = new byte[item.body().remaining()];
        item.body().get(bytes);
        Object obj = Serializer.decode(bytes, Object.class);
        processor.process(obj);
      }
    }
    stopped = true;
  }

  @Override
  protected void cancelTask() throws Exception {
    running = false;
    while (!stopped) {
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("InputStreamTask{");
    sb.append("taskId=").append(taskId);
    sb.append(", processor=").append(processor);
    sb.append('}');
    return sb.toString();
  }
}
