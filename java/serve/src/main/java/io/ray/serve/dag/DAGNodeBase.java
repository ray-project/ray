package io.ray.serve.dag;

import java.util.Map;
import java.util.function.Function;

public interface DAGNodeBase {

  <T> T applyRecursive(Function<DAGNodeBase, T> fn);

  <T> DAGNodeBase applyAndReplaceAllChildNodes(Function<DAGNodeBase, T> fn);

  DAGNodeBase copy(
      Object[] newArgs, Map<String, Object> newOptions, Map<String, Object> newOtherArgsToResolve);

  default DAGNodeBase copyImpl(
      Object[] newArgs, Map<String, Object> newOptions, Map<String, Object> newOtherArgsToResolve) {
    throw new UnsupportedOperationException();
  }

  String getStableUuid();
}
