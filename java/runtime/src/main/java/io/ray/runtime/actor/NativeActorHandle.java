package io.ray.runtime.actor;

import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.runtime.AbstractRayRuntime;
import io.ray.runtime.generated.Common.Language;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.Reference;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract and language-independent implementation of actor handle for cluster mode. This is a
 * wrapper class for C++ ActorHandle.
 */
public abstract class NativeActorHandle implements BaseActorHandle, Externalizable {

  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();

  private static final Set<Reference<NativeActorHandle>> REFERENCES = Sets.newConcurrentHashSet();

  /** ID of the actor. */
  byte[] actorId;

  /** ID of the actor handle. */
  byte[] actorHandleId = new byte[ObjectId.LENGTH];

  private Language language;

  NativeActorHandle(byte[] actorId, Language language) {
    Preconditions.checkState(!ActorId.fromBytes(actorId).isNil());
    this.actorId = actorId;
    this.language = language;
    new NativeActorHandleReference(this);
  }

  /** Required by FST. */
  NativeActorHandle() {
    // Note there is no need to add local reference here since this is only used for FST.
  }

  public ObjectId getActorHandleId() {
    return new ObjectId(actorHandleId);
  }

  public static NativeActorHandle create(byte[] actorId) {
    Language language = Language.forNumber(nativeGetLanguage(actorId));
    Preconditions.checkState(language != null, "Language shouldn't be null");
    return create(actorId, language);
  }

  public static NativeActorHandle create(byte[] actorId, Language language) {
    switch (language) {
      case JAVA:
        return new NativeJavaActorHandle(actorId);
      case PYTHON:
        return new NativePyActorHandle(actorId);
      case CPP:
        return new NativeCppActorHandle(actorId);
      default:
        throw new IllegalStateException("Unknown actor handle language: " + language);
    }
  }

  @Override
  public ActorId getId() {
    return ActorId.fromBytes(actorId);
  }

  public Language getLanguage() {
    return language;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(nativeSerialize(actorId, actorHandleId));
    out.writeObject(language);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    actorId = nativeDeserialize((byte[]) in.readObject());
    language = (Language) in.readObject();
    new NativeActorHandleReference(this);
  }

  /**
   * Serialize this actor handle to bytes.
   *
   * @return the bytes of the actor handle
   */
  public byte[] toBytes() {
    return nativeSerialize(actorId, actorHandleId);
  }

  /**
   * Deserialize an actor handle from bytes.
   *
   * @return the bytes of an actor handle
   */
  public static NativeActorHandle fromBytes(byte[] bytes) {
    byte[] actorId = nativeDeserialize(bytes);
    Language language = Language.forNumber(nativeGetLanguage(actorId));
    Preconditions.checkNotNull(language);
    return create(actorId, language);
  }

  private static final class NativeActorHandleReference
      extends FinalizableWeakReference<NativeActorHandle> {
    private final AtomicBoolean removed;
    private final byte[] actorId;

    public NativeActorHandleReference(NativeActorHandle handle) {
      super(handle, REFERENCE_QUEUE);
      this.actorId = handle.actorId;
      AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
      this.removed = new AtomicBoolean(false);
      REFERENCES.add(this);
    }

    @Override
    public void finalizeReferent() {
      if (!removed.getAndSet(true)) {
        REFERENCES.remove(this);
        // It's possible that GC is executed after the runtime is shutdown.
        if (Ray.isInitialized()) {
          nativeRemoveActorHandleReference(actorId);
        }
      }
    }
  }

  // TODO(chaokunyang) do we need to free the ActorHandle in core worker by using phantom reference?

  private static native int nativeGetLanguage(byte[] actorId);

  static native List<String> nativeGetActorCreationTaskFunctionDescriptor(byte[] actorId);

  private static native byte[] nativeSerialize(byte[] actorId, byte[] actorHandleId);

  private static native byte[] nativeDeserialize(byte[] data);

  private static native void nativeRemoveActorHandleReference(byte[] actorId);
}
