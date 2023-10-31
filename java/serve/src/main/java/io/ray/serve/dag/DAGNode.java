package io.ray.serve.dag;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public abstract class DAGNode implements DAGNodeBase {

  private final Object[] boundArgs;

  private final Map<String, Object> boundOptions;

  private final Map<String, Object> boundOtherArgsToResolve;

  private String stableUuid = UUID.randomUUID().toString().replace("-", "");

  public DAGNode(
      Object[] args, Map<String, Object> options, Map<String, Object> otherArgsToResolve) {
    this.boundArgs = args != null ? args : new Object[0];
    this.boundOptions = options != null ? options : new HashMap<>();
    this.boundOtherArgsToResolve =
        otherArgsToResolve != null ? otherArgsToResolve : new HashMap<>();
  }

  @Override
  public <T> T applyRecursive(Function<DAGNodeBase, T> fn) {
    if (!(fn instanceof CachingFn)) {
      Function<DAGNodeBase, T> newFun = new CachingFn<>(fn);
      return newFun.apply(applyAndReplaceAllChildNodes(node -> node.applyRecursive(newFun)));
    } else {
      return fn.apply(applyAndReplaceAllChildNodes(node -> node.applyRecursive(fn)));
    }
  }

  @Override
  public <T> DAGNodeBase applyAndReplaceAllChildNodes(Function<DAGNodeBase, T> fn) {
    Object[] newArgs = new Object[boundArgs.length];
    for (int i = 0; i < boundArgs.length; i++) {
      newArgs[i] =
          boundArgs[i] instanceof DAGNodeBase ? fn.apply((DAGNodeBase) boundArgs[i]) : boundArgs[i];
    }

    return copy(newArgs, boundOptions, boundOtherArgsToResolve);
  }

  @Override
  public DAGNodeBase copy(
      Object[] newArgs, Map<String, Object> newOptions, Map<String, Object> newOtherArgsToResolve) {
    DAGNode instance = (DAGNode) copyImpl(newArgs, newOptions, newOtherArgsToResolve);
    instance.stableUuid = stableUuid;
    return instance;
  }

  public Object[] getBoundArgs() {
    return boundArgs;
  }

  public Map<String, Object> getBoundOptions() {
    return boundOptions;
  }

  public Map<String, Object> getBoundOtherArgsToResolve() {
    return boundOtherArgsToResolve;
  }

  @Override
  public String getStableUuid() {
    return stableUuid;
  }

  private static class CachingFn<T> implements Function<DAGNodeBase, T> {

    private final Map<String, T> cache = new HashMap<>();

    private final Function<DAGNodeBase, T> function;

    public CachingFn(Function<DAGNodeBase, T> function) {
      this.function = function;
    }

    @Override
    public T apply(DAGNodeBase t) {
      return cache.computeIfAbsent(t.getStableUuid(), key -> function.apply(t));
    }
  }
}
