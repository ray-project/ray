package org.ray.api.exception;

/**
 * Indicates that an object is lost (either evicted or explicitly deleted) and cannot be
 * reconstructed.
 * <p>
 * Note, this exception only happens for actor objects. If actor's current state is after object's
 * creating task, the actor cannot re-run the task to reconstruct the object.
 */
public class UnreconstructableException extends RayException {

  public static final UnreconstructableException INSTANCE = new UnreconstructableException();

  private UnreconstructableException() {
    super("Object is lost (either evicted or explicitly deleted) and cannot be reconstructed.");
  }

}
