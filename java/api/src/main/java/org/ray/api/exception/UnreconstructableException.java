package org.ray.api.exception;

import org.ray.api.id.UniqueId;

/**
 * Indicates that an object is lost and cannot be reconstructed.
 *
 * Note, this exception only happens for actor objects. If actor's current state is after object's
 * creating task, the actor cannot re-run the task to reconstruct the object.
 */
public class UnreconstructableException extends RayException {

  public final UniqueId objectId;

  public UnreconstructableException(UniqueId objectId) {
    super(String.format("Object %s cannot be reconstructed.", objectId));
    this.objectId = objectId;
  }

}
