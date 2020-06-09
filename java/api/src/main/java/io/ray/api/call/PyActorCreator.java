package io.ray.api.call;

import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;

public class PyActorCreator extends BaseActorCreator {
  private final PyActorClass pyActorClass;
  private final Object[] args;

  public PyActorCreator(PyActorClass pyActorClass, Object[] args) {
    this.pyActorClass = pyActorClass;
    this.args = args;
  }

  public PyActorHandle remote() {
    return Ray.internal().createActor(pyActorClass, args, createActorCreationOptions());
  }
}
