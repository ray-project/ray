package org.ray.streaming.runtime.worker.tasks;

import org.ray.runtime.util.Serializer;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.transfer.Message;
import org.ray.streaming.runtime.worker.JobWorker;

public abstract class InputStreamTask extends StreamTask {
  private volatile boolean running = true;

  public InputStreamTask(int taskId, Processor processor, JobWorker streamWorker) {
    super(taskId, processor, streamWorker);
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    while (running) {
      Message item = reader.pull(1000);
      if (item != null) {
        byte[] bytes = new byte[item.body().remaining()];
        item.body().get(bytes);
        Object obj = Serializer.decode(bytes);
        processor.process(obj);
      }
    }
  }

  @Override
  protected void cancelTask() throws Exception {
    running = false;
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
