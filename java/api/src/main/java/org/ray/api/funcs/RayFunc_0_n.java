package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_0_n<R, RIDT> extends RayFunc {

  static <R, RIDT> Map<RIDT, R> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_0_n.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_0_n<R, RIDT> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((Collection<RIDT>) args[0]);
  }

  Map<RIDT, R> apply(Collection<RIDT> returnids) throws Throwable;

}
