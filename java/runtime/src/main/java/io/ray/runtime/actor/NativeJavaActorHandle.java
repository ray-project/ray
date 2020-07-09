package io.ray.runtime.actor;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.runtime.generated.Common.Language;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * Java implementation of actor handle for cluster mode.
 */
public class NativeJavaActorHandle extends NativeActorHandle implements ActorHandle {

  NativeJavaActorHandle(byte[] actorId) {
    super(actorId, Language.JAVA);
  }

  /**
   * Required by FST
   */
  public NativeJavaActorHandle() {
    super();
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    Preconditions.checkState(getLanguage() == Language.JAVA);
  }
}
