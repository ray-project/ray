package io.ray.serve.handle;

import io.ray.api.ObjectRef;

/**
 * A future-like object wrapping the result of a unary deployment handle call.
 *
 * <p>From outside a deployment, `.result()` can be used to retrieve the output in a blocking
 * manner.
 */
public class DeploymentResponse {
  private ObjectRef<Object> objectRef;

  public DeploymentResponse(ObjectRef<Object> objectRef) {
    this.objectRef = objectRef;
  }

  public Object result() {
    return objectRef.get();
  }

  /**
   * Fetch the result of the handle call synchronously.
   *
   * @param timeoutMs The unit is millisecond.
   * @return call result
   */
  public Object result(long timeoutMs) {
    return objectRef.get(timeoutMs);
  }
}
