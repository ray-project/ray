package io.ray.serve.poll;

import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Key type of long poll.
 */
public class KeyType implements Serializable {

  private static final long serialVersionUID = -8838552786234630401L;

  private LongPollNamespace longPollNamespace;

  private String key;

  private int hash;

  public KeyType(LongPollNamespace longPollNamespace, String key) {
    this.longPollNamespace = longPollNamespace;
    this.key = key;
  }

  public LongPollNamespace getLongPollNamespace() {
    return longPollNamespace;
  }

  public void setLongPollNamespace(LongPollNamespace longPollNamespace) {
    this.longPollNamespace = longPollNamespace;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hash(longPollNamespace, key);
    }
    return hash;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }

}
