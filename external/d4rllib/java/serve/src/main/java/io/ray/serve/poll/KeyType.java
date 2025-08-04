package io.ray.serve.poll;

import io.ray.serve.exception.RayServeException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/** Key type of long poll. */
public class KeyType implements Serializable {

  private static final long serialVersionUID = -8838552786234630401L;

  private final LongPollNamespace longPollNamespace;

  private final String key;

  private int hashCode;

  private String name;

  public KeyType(LongPollNamespace longPollNamespace, String key) {
    this.longPollNamespace = longPollNamespace;
    this.key = key;
    this.hashCode = Objects.hash(this.longPollNamespace, this.key);
    this.name = parseName();
  }

  public LongPollNamespace getLongPollNamespace() {
    return longPollNamespace;
  }

  public String getKey() {
    return key;
  }

  @Override
  public int hashCode() {
    return hashCode;
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

  private String parseName() {
    if (longPollNamespace == null && StringUtils.isBlank(key)) {
      return "";
    }
    if (longPollNamespace == null) {
      return key;
    }
    if (StringUtils.isBlank(key)) {
      return longPollNamespace.name();
    }
    return "(" + longPollNamespace.name() + "," + key + ")";
  }

  @Override
  public String toString() {
    return name;
  }

  public static KeyType parseFrom(String key) {
    if (key == null) {
      return null;
    }
    if (StringUtils.isBlank(key)) {
      return new KeyType(null, null);
    }
    if (key.startsWith("(")) {
      String[] fields = StringUtils.split(StringUtils.substring(key, 1, key.length() - 1), ",", 2);
      if (fields.length != 2) {
        throw new RayServeException("Illegal KeyType string: " + key);
      }
      return new KeyType(LongPollNamespace.parseFrom(fields[0]), fields[1].trim());
    }
    LongPollNamespace longPollNamespace = LongPollNamespace.parseFrom(key);
    if (null != longPollNamespace) {
      return new KeyType(longPollNamespace, null);
    }
    return new KeyType(null, key);
  }
}
