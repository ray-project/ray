package org.ray.runtime.actor;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.ObjectInput;
import org.ray.api.RayJavaActor;
import org.ray.runtime.generated.Common.Language;

/**
 * RayActor Java implementation for cluster mode.
 */
public class NativeRayJavaActor extends NativeRayActor implements RayJavaActor {

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
