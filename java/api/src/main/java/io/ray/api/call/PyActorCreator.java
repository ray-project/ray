package io.ray.api.call;

import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;

/** A helper to create python actor. */
public class PyActorCreator extends BaseActorCreator<PyActorCreator> {
  private final PyActorClass pyActorClass;
  private final Object[] args;

  public PyActorCreator(PyActorClass pyActorClass, Object[] args) {
    this.pyActorClass = pyActorClass;
    this.args = args;
  }

  public PyActorCreator setAsync(boolean isAsync) {
    builder.setAsync(isAsync);
    return this;
  }

  /**
   * Create a python actor remotely and return a handle to the created actor.
   *
   * @return a handle to the created python actor.
   */
  public PyActorHandle remote() {
    return Ray.internal().createActor(pyActorClass, args, buildOptions());
  }
}
