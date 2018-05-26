package org.ray.api.returns;

import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;

@SuppressWarnings({"rawtypes", "unchecked"})
public class RayObjects2<R0, R1> extends RayObjects {

  public RayObjects2(UniqueID[] ids) {
    super(ids);
  }

  public RayObjects2(RayObject objs[]) {
    super(objs);
  }

  public RayObject<R0> r0() {
    return objs[0];
  }

  public RayObject<R1> r1() {
    return objs[1];
  }
}
