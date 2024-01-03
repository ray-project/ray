package io.ray.api.runtimecontext;

import io.ray.api.id.UniqueId;
import java.util.Objects;

public class Address {
  public final UniqueId nodeId;
  public final String ip;
  public final int port;

  public Address(UniqueId nodeId, String ip, int port) {
    this.nodeId = nodeId;
    this.ip = ip;
    this.port = port;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!this.getClass().equals(obj.getClass())) {
      return false;
    }

    Address other = (Address) obj;
    return this.nodeId.equals(other.nodeId) && this.ip.equals(other.ip) && this.port == other.port;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId, ip, port);
  }
}
