package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.parallelactor.ParallelActorHandle;
import io.ray.api.parallelactor.ParallelActorInstance;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ParallelActorHandleImpl<A> implements ParallelActorHandle<A>, Externalizable {

  private int parallelism = 1;

  private ActorHandle<ParallelActorExecutorImpl> parallelExecutorHandle = null;

  // An empty ctor for FST serializing need.
  public ParallelActorHandleImpl() {}

  public ParallelActorHandleImpl(int parallelism, ActorHandle<ParallelActorExecutorImpl> handle) {
    this.parallelism = parallelism;
    parallelExecutorHandle = handle;
  }

  @Override
  public ParallelActorInstance<A> getInstance(int index) {
    return new ParallelActorInstance<A>(this, index);
  }

  public ActorHandle<? extends ParallelActorExecutorImpl> getExecutor() {
    return parallelExecutorHandle;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public ActorHandle<?> getHandle() {
    return parallelExecutorHandle;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(parallelExecutorHandle);
    out.writeInt(parallelism);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.parallelExecutorHandle = (ActorHandle<ParallelActorExecutorImpl>) in.readObject();
    this.parallelism = in.readInt();
  }
}
