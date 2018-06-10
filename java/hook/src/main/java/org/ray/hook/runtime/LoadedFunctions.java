package org.ray.hook.runtime;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.ray.hook.MethodId;

public class LoadedFunctions {

  public final Set<MethodId> functions = Collections.synchronizedSet(new HashSet<>());
  public ClassLoader loader = null;
}
