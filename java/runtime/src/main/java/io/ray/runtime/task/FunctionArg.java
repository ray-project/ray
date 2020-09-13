package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import io.ray.api.id.ObjectId;
import io.ray.runtime.object.NativeRayObject;

/**
 * Represents a function argument in task spec.
 * Either `id` or `data` should be null, when id is not null, this argument will be
 * passed by reference, otherwise it will be passed by value.
 */
public class FunctionArg {

  /**
   * The id of this argument (passed by reference).
   */
  public final ObjectId id;
  /**
   * Serialized data of this argument (passed by value).
   */
  public final NativeRayObject value;

  private FunctionArg(ObjectId id, NativeRayObject value) {
    Preconditions.checkState((id == null) != (value == null));
    this.id = id;
    this.value = value;
  }

  /**
   * Create a FunctionArg that will be passed by reference.
   */
  public static FunctionArg passByReference(ObjectId id) {
    return new FunctionArg(id, null);
  }

  /**
   * Create a FunctionArg that will be passed by value.
   */
  public static FunctionArg passByValue(NativeRayObject value) {
    return new FunctionArg(null, value);
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
