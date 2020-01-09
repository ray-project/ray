package org.ray.lib.service.api;

import java.util.List;
import org.ray.api.RayActor;
import org.ray.lib.service.api.id.ServiceId;

/**
 * A handle to a Service.
 *
 * @param <T> The type of the concrete Actor class in the service.
 */
public interface RayService<T> {

  /**
   * Get the id of this Service.
   */
  ServiceId getId();

  /**
   * Get the name of this Service.
   */
  String getName();

  /**
   * Get the sorted Actors belonging to this Service.
   */
  List<RayActor<T>> getActors();
}
