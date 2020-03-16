package org.ray.runtime.actor;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.ObjectInput;
import org.ray.api.RayActor;
import org.ray.runtime.generated.Common.Language;

/**
 * Java implementation of actor handle for cluster mode.
 */
public class NativeRayJavaActor extends NativeRayActor implements RayActor {

  NativeRayJavaActor(long nativeCoreWorkerPointer, byte[] actorId) {
    super(nativeCoreWorkerPointer, actorId);
  }

  /**
   * Required by FST
   */
  public NativeRayJavaActor() {
    super();
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    Preconditions.checkState(getLanguage() == Language.JAVA);
  }
}
