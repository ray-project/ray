package io.ray.streaming.runtime.worker.tasks;

import com.google.common.base.MoreObjects;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.serialization.CrossLangSerializer;
import io.ray.streaming.runtime.serialization.JavaSerializer;
import io.ray.streaming.runtime.serialization.Serializer;
import io.ray.streaming.runtime.transfer.Message;
import io.ray.streaming.runtime.worker.JobWorker;

public abstract class InputStreamTask extends StreamTask {
  private volatile boolean running = true;
  private volatile boolean stopped = false;
  private long readTimeoutMillis;
  private final io.ray.streaming.runtime.serialization.Serializer javaSerializer;
  private final io.ray.streaming.runtime.serialization.Serializer crossLangSerializer;

  public InputStreamTask(int taskId, Processor processor, JobWorker jobWorker) {
    super(taskId, processor, jobWorker);
    readTimeoutMillis = jobWorker.getWorkerConfig().transferConfig.readerTimerIntervalMs();
    javaSerializer = new JavaSerializer();
    crossLangSerializer = new CrossLangSerializer();
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    while (running) {
      Message item = reader.read(readTimeoutMillis);
      if (item != null) {
        byte[] bytes = new byte[item.body().remaining() - 1];
        byte typeId = item.body().get();
        item.body().get(bytes);
        Object obj;
        if (typeId == Serializer.JAVA_TYPE_ID) {
          obj = javaSerializer.deserialize(bytes);
        } else {
          obj = crossLangSerializer.deserialize(bytes);
        }
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
    return MoreObjects.toStringHelper(this)
      .add("taskId", taskId)
      .add("processor", processor)
      .toString();
  }
}
