package io.ray.serve.poll;

import io.ray.serve.RayServeException;
import org.apache.commons.lang3.StringUtils;

/** The long poll namespace enum. */
public enum LongPollNamespace {
  REPLICA_HANDLES,

  ROUTE_TABLE;

  public static LongPollNamespace parseFrom(String key) {
    String[] namespaces = StringUtils.split(key, ".");
    if (namespaces.length != 2) {
      throw new RayServeException("Illegal KeyType string: " + key);
    }
    return valueOf(namespaces[1].trim());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "." + this.name();
  }
}
