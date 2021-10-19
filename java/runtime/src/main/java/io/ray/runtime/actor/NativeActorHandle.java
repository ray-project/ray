package io.ray.runtime.actor;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.runtime.generated.Common.Language;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Abstract and language-independent implementation of actor handle for cluster mode. This is a
 * wrapper class for C++ ActorHandle.
 */
public abstract class NativeActorHandle implements BaseActorHandle, Externalizable {

  /** ID of the actor. */
  byte[] actorId;

  private Language language;

  NativeActorHandle(byte[] actorId, Language language) {
    Preconditions.checkState(!ActorId.fromBytes(actorId).isNil());
    this.actorId = actorId;
    this.language = language;
  }

  /** Required by FST. */
  NativeActorHandle() {}

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
    out.writeObject(nativeSerialize(actorId));
    out.writeObject(language);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    actorId = nativeDeserialize((byte[]) in.readObject());
    language = (Language) in.readObject();
  }

  /**
   * Serialize this actor handle to bytes.
   *
   * @return the bytes of the actor handle
   */
  public byte[] toBytes() {
    return nativeSerialize(actorId);
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

  // TODO(chaokunyang) do we need to free the ActorHandle in core worker by using phantom reference?

  private static native int nativeGetLanguage(byte[] actorId);

  static native List<String> nativeGetActorCreationTaskFunctionDescriptor(byte[] actorId);

  private static native byte[] nativeSerialize(byte[] actorId);

  private static native byte[] nativeDeserialize(byte[] data);
}
