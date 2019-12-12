package org.ray.streaming.runtime.streamingqueue;

import org.ray.api.Ray;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.streaming.runtime.transfer.TransferHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  protected TransferHandler transferHandler = null;
  public Worker() {
    transferHandler = new TransferHandler(((RayMultiWorkerNativeRuntime) Ray.internal()).getCurrentRuntime().getNativeCoreWorkerPointer(),
        new JavaFunctionDescriptor(Worker.class.getName(), "onWriterMessage", "([B)V"),
        new JavaFunctionDescriptor(Worker.class.getName(), "onWriterMessageSync", "([B)[B"),
        new JavaFunctionDescriptor(Worker.class.getName(), "onReaderMessage", "([B)V"),
        new JavaFunctionDescriptor(Worker.class.getName(), "onReaderMessageSync", "([B)[B"));
  }

  public void onReaderMessage(byte[] buffer) {
    transferHandler.onReaderMessage(buffer);
  }

  public byte[] onReaderMessageSync(byte[] buffer) {
    return transferHandler.onReaderMessageSync(buffer);
  }

  public void onWriterMessage(byte[] buffer) {
    transferHandler.onWriterMessage(buffer);
  }

  public byte[] onWriterMessageSync(byte[] buffer) {
    return transferHandler.onWriterMessageSync(buffer);
  }
}
