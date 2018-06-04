package org.ray.api.benchmark;

import org.ray.api.RayObject;

public class RemoteResultWrapper<T> {

  private long startTime;

  private RayObject<RemoteResult<T>> rayObject;

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public RayObject<RemoteResult<T>> getRayObject() {
    return rayObject;
  }

  public void setRayObject(RayObject<RemoteResult<T>> rayObject) {
    this.rayObject = rayObject;
  }
}
