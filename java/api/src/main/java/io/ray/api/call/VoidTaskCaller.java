package io.ray.api.call;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;
import java.util.HashMap;
import java.util.Map;

public class VoidTaskCaller extends BaseCaller {
  private final RayFuncVoid func;
  private final Object[] args;

  public VoidTaskCaller(RayFuncVoid func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  public void remote() {
    Ray.internal().call(func, args, createCallOptions());
  }

}
