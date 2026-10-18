package io.ray.runtime.actor;

import com.google.common.base.Preconditions;
import io.ray.api.PyActorHandle;
import io.ray.runtime.generated.Common.Language;
import java.io.IOException;
import java.io.ObjectInput;

/** Python actor handle implementation for cluster mode. */
public class NativePyActorHandle extends NativeActorHandle implements PyActorHandle {

  // Preserve compatibility with handles serialized before weak-reference cleanup.
  private static final long serialVersionUID = -7021691998330060618L;

  NativePyActorHandle(byte[] actorId) {
    this(actorId, false);
  }

  NativePyActorHandle(byte[] actorId, boolean weakRef) {
    super(actorId, Language.PYTHON, weakRef);
  }

  /** Required by FST. */
  public NativePyActorHandle() {
    super();
  }

  @Override
  public String getModuleName() {
    return nativeGetActorCreationTaskFunctionDescriptor(actorId).get(0);
  }

  @Override
  public String getClassName() {
    return nativeGetActorCreationTaskFunctionDescriptor(actorId).get(1);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    Preconditions.checkState(getLanguage() == Language.PYTHON);
  }
}
