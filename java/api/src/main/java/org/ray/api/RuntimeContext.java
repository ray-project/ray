package org.ray.api;

import org.ray.api.id.UniqueId;

/**
 * A class used for getting runtime context fields.
 */
public interface RuntimeContext {

  UniqueId getCurrentDriverId();

  UniqueId getCurrentActorId();

  boolean wasCurrentActorReconstructed();

  String getRayletSocketName();

  String getObjectStoreSocketName();

}
