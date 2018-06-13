package org.ray.api.funcs;

import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_0_n_list<R> extends RayFunc {

  static <R> List<R> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_0_n_list.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_0_n_list<R> f = SerializationUtils.deserialize(funcBytes);
    return f.apply();
  }

  List<R> apply() throws Throwable;

}
