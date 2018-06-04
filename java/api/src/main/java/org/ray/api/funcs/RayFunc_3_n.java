package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_3_n<T0, T1, T2, R, RID> extends RayFunc {

  Map<RID, R> apply(Collection<RID> returnids, T0 t0, T1 t1, T2 t2) throws Throwable;

  static <T0, T1, T2, R, RID> Map<RID, R> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_3_n.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_3_n<T0, T1, T2, R, RID> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((Collection<RID>) args[0], (T0) args[1], (T1) args[2], (T2) args[3]);
  }

}
