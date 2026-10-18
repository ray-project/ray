package io.ray.runtime.actor;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.runtime.generated.Common.Language;
import java.io.IOException;
import java.io.ObjectInput;

/** Java implementation of actor handle for cluster mode. */
public class NativeJavaActorHandle extends NativeActorHandle implements ActorHandle {

  // Preserve compatibility with handles serialized before weak-reference cleanup.
  private static final long serialVersionUID = 6318953115892572541L;

  NativeJavaActorHandle(byte[] actorId) {
    this(actorId, false);
  }

  NativeJavaActorHandle(byte[] actorId, boolean weakRef) {
    super(actorId, Language.JAVA, weakRef);
  }

  /** Required by FST. */
  public NativeJavaActorHandle() {
    super();
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    Preconditions.checkState(getLanguage() == Language.JAVA);
  }
}
