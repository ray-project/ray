package org.ray.hook.runtime;

import java.util.Arrays;

public class MethodHash {

  private final byte[] hash;

  public MethodHash(byte[] hash) {
    this.hash = hash;
  }

  public byte[] getHash() {
    return hash;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(getHash());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MethodHash other = (MethodHash) obj;
    return Arrays.equals(this.getHash(), other.getHash());
  }
}
