package org.ray.runtime.actor;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.ObjectInput;
import org.ray.api.RayPyActor;
import org.ray.runtime.generated.Common.Language;

/**
 * Python actor handle implementation for cluster mode.
 */
public class NativeRayPyActor extends NativeRayActor implements RayPyActor {

  NativeRayPyActor(long nativeCoreWorkerPointer, byte[] actorId) {
    super(nativeCoreWorkerPointer, actorId);
  }

  /**
   * Required by FST
   */
  public NativeRayPyActor() {
    super();
  }

  @Override
  public String getModuleName() {
    return nativeGetActorCreationTaskFunctionDescriptor(nativeCoreWorkerPointer, actorId).get(0);
  }

  @Override
  public String getClassName() {
    return nativeGetActorCreationTaskFunctionDescriptor(nativeCoreWorkerPointer, actorId).get(1);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    Preconditions.checkState(getLanguage() == Language.PYTHON);
  }
}
