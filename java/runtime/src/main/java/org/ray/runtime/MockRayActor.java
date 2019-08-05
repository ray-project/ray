package org.ray.runtime;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicReference;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.Language;

public class MockRayActor extends AbstractRayActor implements Externalizable {

  private UniqueId actorId;

  private AtomicReference<ObjectId> previousActorTaskDummyObjectId = new AtomicReference<>();

  public MockRayActor(UniqueId actorId) {
    this.actorId = actorId;
    this.previousActorTaskDummyObjectId.set(new ObjectId(actorId.getBytes()));
  }

  public MockRayActor() {
  }

  @Override
  public UniqueId getId() {
    return actorId;
  }

  @Override
  public UniqueId getHandleId() {
    return UniqueId.NIL;
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }

  public ObjectId exchangePreviousActorTaskDummyObjectId(ObjectId previousActorTaskDummyObjectId) {
    return this.previousActorTaskDummyObjectId.getAndSet(previousActorTaskDummyObjectId);
  }

  @Override
  public synchronized void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(actorId);
    out.writeObject(previousActorTaskDummyObjectId.get());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    actorId = (UniqueId) in.readObject();
    previousActorTaskDummyObjectId.set((ObjectId) in.readObject());
  }
}
