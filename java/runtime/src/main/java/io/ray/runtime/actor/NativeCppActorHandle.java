package io.ray.runtime.actor;

import com.google.common.base.Preconditions;
import io.ray.api.CppActorHandle;
import io.ray.runtime.generated.Common.Language;
import java.io.IOException;
import java.io.ObjectInput;

/** Cpp actor handle implementation for cluster mode. */
public class NativeCppActorHandle extends NativeActorHandle implements CppActorHandle {

  NativeCppActorHandle(byte[] actorId) {
    super(actorId, Language.CPP);
  }

  /** Required by FST. */
  public NativeCppActorHandle() {
    super();
  }

  @Override
  public String getClassName() {
    return nativeGetActorCreationTaskFunctionDescriptor(actorId).get(2);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    Preconditions.checkState(getLanguage() == Language.CPP);
  }
}
