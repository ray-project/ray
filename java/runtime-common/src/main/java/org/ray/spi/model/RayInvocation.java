package org.ray.spi.model;

import org.ray.api.RayActor;
import org.ray.api.UniqueID;

/**
 * Represents an invocation of ray remote function.
 */
public class RayInvocation {

  private static final RayActor<?> nil = new RayActor<>(UniqueID.nil, UniqueID.nil);
  /**
   * unique id for a method.
   *
   * @see UniqueID
   */
  private final byte[] id;
  private final RayActor<?> actor;
  /**
   * function arguments.
   */
  private Object[] args;

  public RayInvocation(byte[] id, Object[] args) {
    this(id, args, nil);
  }

  public RayInvocation(byte[] id, Object[] args, RayActor<?> actor) {
    super();
    this.id = id;
    this.args = args;
    this.actor = actor;
  }

  public byte[] getId() {
    return id;
  }

  public Object[] getArgs() {
    return args;
  }

  public void setArgs(Object[] args) {
    this.args = args;
  }

  public RayActor<?> getActor() {
    return actor;
  }

}
