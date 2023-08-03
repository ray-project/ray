package io.ray.api.runtimecontext;

/** A class that contains resource id and capacity of this resource. */
public class ResourceValue {

  public final Long resourceId;

  public final Double capacity;

  ResourceValue(long resourceId, double capacity) {
    this.resourceId = resourceId;
    this.capacity = capacity;
  }
}
