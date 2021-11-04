package io.ray.runtime.object;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/// A POJO that's used to inline some serialized information for object ownership.
public final class OwnershipInfo implements Externalizable {

  private byte[] serializedOwnerAddress;

  private byte[] serializedObjectStatus;

  public OwnershipInfo() {

  }

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

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(serializedOwnerAddress.length);
    out.write(serializedOwnerAddress);
    out.writeInt(serializedObjectStatus.length);
    out.write(serializedObjectStatus);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    int ownerAddressBytesLen = in.readInt();
    this.serializedOwnerAddress = new byte[ownerAddressBytesLen];
    in.readFully(serializedOwnerAddress);
    int objectStatusBytesLen = in.readInt();
    this.serializedObjectStatus = new byte[objectStatusBytesLen];
  }
}
