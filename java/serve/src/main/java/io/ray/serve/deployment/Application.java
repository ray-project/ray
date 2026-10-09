package io.ray.serve.deployment;

import io.ray.serve.dag.DAGNode;
import io.ray.serve.dag.DAGNodeBase;
import java.util.Map;
import java.util.function.Function;

public class Application implements DAGNodeBase {
  private DAGNode internalDagNode;

  private Application(DAGNode internalDagNode) {
    this.internalDagNode = internalDagNode;
  }

  public DAGNode getInternalDagNode() {
    return internalDagNode;
  }

  public static Application fromInternalDagNode(DAGNode dagNode) {
    return new Application(dagNode);
  }

  @Override
  public <T> T applyRecursive(Function<DAGNodeBase, T> fn) {
    return internalDagNode.applyRecursive(fn);
  }

  @Override
  public <T> DAGNodeBase applyAndReplaceAllChildNodes(Function<DAGNodeBase, T> fn) {
    return internalDagNode.applyAndReplaceAllChildNodes(fn);
  }

  @Override
  public DAGNodeBase copy(
      Object[] newArgs, Map<String, Object> newOptions, Map<String, Object> newOtherArgsToResolve) {
    return internalDagNode.copy(newArgs, newOptions, newOtherArgsToResolve);
  }

  @Override
  public String getStableUuid() {
    return internalDagNode.getStableUuid();
  }
}
