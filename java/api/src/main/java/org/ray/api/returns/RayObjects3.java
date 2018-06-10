package org.ray.api.returns;

import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;

@SuppressWarnings({"rawtypes", "unchecked"})
public class RayObjects3<R0, R1, R2> extends RayObjects {

  public RayObjects3(UniqueID[] ids) {
    super(ids);
  }

  public RayObjects3(RayObject[] objs) {
    super(objs);
  }

  public RayObject<R0> r0() {
    return objs[0];
  }

  public RayObject<R1> r1() {
    return objs[1];
  }

  public RayObject<R2> r2() {
    return objs[2];
  }
}
