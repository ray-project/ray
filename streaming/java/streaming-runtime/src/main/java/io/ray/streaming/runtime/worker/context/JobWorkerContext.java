package io.ray.streaming.runtime.worker.context;

import java.io.Serializable;

/**
 * Abstract class for job worker context.
 * Only support java and python job worker context for now.
 */
public abstract class JobWorkerContext implements Serializable {

  /**
   * Get worker context in bytes array.
   *
   * @return bytes
   */
  public abstract byte[] getContextBytes();

}
