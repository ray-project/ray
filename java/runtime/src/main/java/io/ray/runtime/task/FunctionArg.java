package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import io.ray.api.id.ObjectId;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.object.NativeRayObject;

/**
 * Represents a function argument in task spec. Either `id` or `data` should be null, when id is not
 * null, this argument will be passed by reference, otherwise it will be passed by value.
 */
public class FunctionArg {

  /** The id of this argument (passed by reference). */
  public final ObjectId id;

  /** The owner address of this argument (passed by reference). */
  public final Address ownerAddress;

  /** Serialized data of this argument (passed by value). */
  public final NativeRayObject value;

  private FunctionArg(ObjectId id, Address ownerAddress) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(ownerAddress);
    this.id = id;
    this.ownerAddress = ownerAddress;
    this.value = null;
  }

  private FunctionArg(NativeRayObject nativeRayObject) {
    Preconditions.checkNotNull(nativeRayObject);
    this.id = null;
    this.ownerAddress = null;
    this.value = nativeRayObject;
  }

  /** Create a FunctionArg that will be passed by reference. */
  public static FunctionArg passByReference(ObjectId id, Address ownerAddress) {
    return new FunctionArg(id, ownerAddress);
  }

  /** Create a FunctionArg that will be passed by value. */
  public static FunctionArg passByValue(NativeRayObject value) {
    return new FunctionArg(value);
  }

  @Override
  public String toString() {
    if (id != null) {
      return "<id>: " + id.toString();
    } else {
      return value.toString();
    }
  }
}
