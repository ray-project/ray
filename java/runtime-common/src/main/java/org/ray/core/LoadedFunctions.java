package org.ray.core;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.ray.util.MethodId;

public class LoadedFunctions {

  public final ClassLoader loader;
  public final Set<MethodId> functions = Collections.synchronizedSet(new HashSet<>());

  public LoadedFunctions(ClassLoader loader, Set<MethodId> methods) {
    this.loader = loader;
    this.functions.addAll(methods);
  }

}
