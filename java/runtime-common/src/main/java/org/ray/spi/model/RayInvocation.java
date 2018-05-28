package org.ray.spi.model;

import org.ray.api.RayActor;
import org.ray.api.UniqueID;

/**
 * Represents an invocation of ray remote function.
 */
public class RayInvocation {

  /**
   * unique id for a method
   *
   * @see UniqueID
   */
  private final byte[] id;
  /**
   * function arguments
   */
  private Object[] args;

  private final RayActor<?> actor;

  private static final RayActor<?> nil = new RayActor<>(UniqueID.nil, UniqueID.nil);

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
