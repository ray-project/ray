package io.ray.serve.poll;

import org.apache.commons.lang3.StringUtils;

/** The long poll namespace enum. */
public enum LongPollNamespace {
  RUNNING_REPLICAS,

  ROUTE_TABLE;

  public static LongPollNamespace parseFrom(String key) {
    for (LongPollNamespace namespace : LongPollNamespace.values()) {
      if (StringUtils.equals(key, namespace.name())) {
        return namespace;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "." + this.name();
  }
}
