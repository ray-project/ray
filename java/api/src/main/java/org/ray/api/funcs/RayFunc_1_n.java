package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_1_n<T0, R, RID> extends RayFunc {

  Map<RID, R> apply(Collection<RID> returnids, T0 t0) throws Throwable;

  static <T0, R, RID> Map<RID, R> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_1_n.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_1_n<T0, R, RID> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((Collection<RID>) args[0], (T0) args[1]);
  }

}
