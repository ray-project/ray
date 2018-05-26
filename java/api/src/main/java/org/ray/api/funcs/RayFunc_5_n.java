package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> extends RayFunc {

  Map<RID, R> apply(Collection<RID> returnids, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) throws Throwable;

  static <T0, T1, T2, T3, T4, R, RID> Map<RID, R> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_5_n.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f = SerializationUtils.deserialize(funcBytes);
    return f
        .apply((Collection<RID>) args[0], (T0) args[1], (T1) args[2], (T2) args[3], (T3) args[4],
            (T4) args[5]);
  }

}
