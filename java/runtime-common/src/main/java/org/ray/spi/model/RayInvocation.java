package org.ray.spi.model;

import org.ray.api.RayActor;
import org.ray.api.RayRemote;
import org.ray.api.UniqueID;

/**
 * Represents an invocation of ray remote function.
 */
public class RayInvocation {

  private static final RayActor<?> nil = new RayActor<>(UniqueID.NIL, UniqueID.NIL);
  public final String className;
  /**
   * unique id for a method.
   */
  private final UniqueID id;
  private final RayRemote remoteAnnotation;
  /**
   * function arguments.
   */
  private Object[] args;


  private RayActor<?> actor;

  public RayInvocation(String className, UniqueID id, Object[] args, RayRemote remoteAnnotation,
      RayActor<?> actor) {
    this.className = className;
    this.id = id;
    this.args = args;
    this.actor = actor;
    this.remoteAnnotation = remoteAnnotation;
  }

  public UniqueID getId() {
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

  public RayRemote getRemoteAnnotation() {
    return remoteAnnotation;
  }

}