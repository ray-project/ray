package io.ray.serve.poll;

import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/** Key type of long poll. */
public class KeyType implements Serializable {

  private static final long serialVersionUID = -8838552786234630401L;

  private final LongPollNamespace longPollNamespace;

  private final String key;

  private int hash;

  public KeyType(LongPollNamespace longPollNamespace, String key) {
    this.longPollNamespace = longPollNamespace;
    this.key = key;
  }

  public LongPollNamespace getLongPollNamespace() {
    return longPollNamespace;
  }

  public String getKey() {
    return key;
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hash(longPollNamespace, key);
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    KeyType keyType = (KeyType) obj;
    return Objects.equals(longPollNamespace, keyType.getLongPollNamespace())
        && Objects.equals(key, keyType.getKey());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
