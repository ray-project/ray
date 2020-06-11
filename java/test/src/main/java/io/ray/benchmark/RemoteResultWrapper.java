package io.ray.benchmark;

import io.ray.api.ObjectRef;

public class RemoteResultWrapper<T> {

  private long startTime;

  private ObjectRef<RemoteResult<T>> objectRef;

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public ObjectRef<RemoteResult<T>> getObjectRef() {
    return objectRef;
  }

  public void setObjectRef(ObjectRef<RemoteResult<T>> objectRef) {
    this.objectRef = objectRef;
  }
}
