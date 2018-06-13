package org.ray.api.funcs;

import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_2_n_list<T0, T1, R> extends RayFunc {

  static <T0, T1, R> List<R> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_2_n_list.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_2_n_list<T0, T1, R> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((T0) args[0], (T1) args[1]);
  }

  List<R> apply(T0 t0, T1 t1) throws Throwable;

}
