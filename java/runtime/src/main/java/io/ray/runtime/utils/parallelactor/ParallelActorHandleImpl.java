package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.parallelactor.ParallelActorHandle;
import io.ray.api.parallelactor.ParallelInstance;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ParallelActorHandleImpl<A> implements ParallelActorHandle<A>, Externalizable {

  private int parallelNum = 1;

  private ActorHandle<ParallelActorExecutorImpl> parallelExecutorHandle = null;

  // An empty ctor for FST serializing need.
  public ParallelActorHandleImpl() {}

  public ParallelActorHandleImpl(int parallelNum, ActorHandle<ParallelActorExecutorImpl> handle) {
    this.parallelNum = parallelNum;
    parallelExecutorHandle = handle;
  }

  @Override
  public ParallelInstance<A> getInstance(int index) {
    return new ParallelInstance<A>(this, index);
  }

  public ActorHandle<? extends ParallelActorExecutorImpl> getExecutor() {
    return parallelExecutorHandle;
  }

  @Override
  public int getParallelNum() {
    return parallelNum;
  }

  @Override
  public ActorHandle<?> getHandle() {
    return parallelExecutorHandle;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(parallelExecutorHandle);
    out.writeInt(parallelNum);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.parallelExecutorHandle = (ActorHandle<ParallelActorExecutorImpl>) in.readObject();
    this.parallelNum = in.readInt();
  }
}
