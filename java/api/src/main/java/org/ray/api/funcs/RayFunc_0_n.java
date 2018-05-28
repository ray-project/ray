package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_0_n<R, RID> extends RayFunc {

  Map<RID, R> apply(Collection<RID> returnids) throws Throwable;

  static <R, RID> Map<RID, R> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_0_n.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_0_n<R, RID> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((Collection<RID>) args[0]);
  }

}
