package org.ray.lib.service.api;

import org.ray.lib.service.api.runtime.RayLibServiceRuntime;

public class RayLibService extends RayServiceCall {

  private static RayLibServiceRuntime rayLibServiceRuntime = null;

  public static void init() {
    //TODO(yuyiming): implement it
  }

  public static RayLibServiceRuntime internal() {
    return rayLibServiceRuntime;
  }
}
