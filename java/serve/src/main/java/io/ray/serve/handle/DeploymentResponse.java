package io.ray.serve.handle;

import io.ray.api.ObjectRef;

public class DeploymentResponse {
  private ObjectRef<Object> objectRef;

  public DeploymentResponse(ObjectRef<Object> objectRef) {
    this.objectRef = objectRef;
  }

  public Object result() {
    return objectRef.get();
  }

  public Object result(long timeoutMs) {
    return objectRef.get(timeoutMs);
  }
}
