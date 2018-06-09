package org.ray.api;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Real object or ray future proxy for multiple returns.
 */
public class RayObjects {

  protected RayObject[] objs;

  public RayObjects(UniqueID[] ids) {
    this.objs = new RayObject[ids.length];
    for (int k = 0; k < ids.length; k++) {
      this.objs[k] = new RayObject<>(ids[k]);
    }
  }

  public RayObjects(RayObject[] objs) {
    this.objs = objs;
  }

  public RayObject pop() {
    RayObject lastObj = objs[objs.length - 1];
    objs = ArrayUtils.subarray(objs, 0, objs.length - 1);
    return lastObj;
  }

  public RayObject[] getObjs() {
    return objs;
  }
}
