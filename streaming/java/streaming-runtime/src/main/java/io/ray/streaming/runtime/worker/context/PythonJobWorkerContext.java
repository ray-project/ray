package io.ray.streaming.runtime.worker.context;

/**
 * Job worker context of python type.
 */
public class PythonJobWorkerContext extends JobWorkerContext {

  private byte[] contextBytes;

  @Override
  public byte[] getContextBytes() {
    return contextBytes;
  }

  public void setContextBytes(byte[] contextBytes) {
    this.contextBytes = contextBytes;
  }
}
