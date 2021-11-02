package io.ray.runtime.object;

/// A POJO that's used to inline some serialized information for object ownership.
public final class OwnershipInfo {

  private byte[] serializedOwnerAddress;

  private byte[] serializedObjectStatus;

  public OwnershipInfo(byte[] serializedOwnerAddress, byte[] serializedObjectStatus) {
    this.serializedOwnerAddress = serializedOwnerAddress;
    this.serializedObjectStatus = serializedObjectStatus;
  }

  public byte[] getSerializedOwnerAddress() {
    return serializedOwnerAddress;
  }

  public byte[] getSerializedObjectStatus() {
    return serializedObjectStatus;
  }
}
