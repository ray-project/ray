package org.ray.lib.service.api;

import java.util.List;
import org.ray.api.RayObject;
import org.ray.api.function.RayFunc1;
import org.ray.api.function.RayFunc2;
import org.ray.lib.service.api.options.ServiceCallOptions;
import org.ray.lib.service.api.options.ServiceCreationOptions;

class RayServiceCall {

  public static <T0, S> RayService<S> openService(
      RayFunc1<T0, S> f, T0 t0, ServiceCreationOptions options) {
    Object[] args = new Object[]{t0};
    return RayLibService.internal().openService(f, args, options);
  }

  public static <T0, S> RayService<S> createService(RayFunc1<T0, S> f, T0 t0,
      ServiceCreationOptions options) {
    Object[] args = new Object[]{t0};
    return RayLibService.internal().openService(f, args, options);
  }

  public static <S, T0, R> RayObject<R> call(RayFunc2<S, T0, R> f, RayService<S> service, T0 t0,
      ServiceCallOptions options) {
    Object[] args = new Object[]{t0};
    return RayLibService.internal().call(f, service, args, options);
  }

  public static <S, T0, R> List<RayObject<R>> broadcast(RayFunc2<S, T0, R> f, RayService<S> service,
      T0 t0, ServiceCallOptions options) {
    return null;
  }
}
