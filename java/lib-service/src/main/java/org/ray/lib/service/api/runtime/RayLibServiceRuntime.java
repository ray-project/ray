package org.ray.lib.service.api.runtime;

import org.ray.api.RayObject;
import org.ray.api.function.RayFunc;
import org.ray.lib.service.api.RayService;
import org.ray.lib.service.api.options.ServiceCallOptions;
import org.ray.lib.service.api.options.ServiceCreationOptions;

public interface RayLibServiceRuntime {

  /**
   * Invoke a remote function on a Service.
   *
   * @param func The remote Service function to run.
   * @param service A handle to the Service.
   * @param args The arguments of the remote function.
   * @param options The options for this call.
   * @return The result object.
   */
  RayObject call(RayFunc func, RayService<?> service, Object[] args, ServiceCallOptions options);

  /**
   * Create a new Service or get a handle of an existing Service.
   *
   * @param actorFactoryFunc A remote function whose return value is an Actor object of this
   * Service.
   * @param args The arguments for the remote function.
   * @param options The options for creating Service.
   * @return A handle to the Service.
   */
  <T> RayService<T> openService(RayFunc actorFactoryFunc, Object[] args,
      ServiceCreationOptions options);
}
