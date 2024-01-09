package io.ray.serve.dag;

import java.util.Map;

public class ClassNode extends DAGNode {

  private String className;

  public ClassNode(
      String clsName,
      Object[] clsArgs,
      Map<String, Object> clsOptions,
      Map<String, Object> otherArgsToResolve) {
    super(clsArgs, clsOptions, otherArgsToResolve);
    this.className = clsName;
  }

  @Override
  public DAGNode copyImpl(
      Object[] newArgs, Map<String, Object> newOptions, Map<String, Object> newOtherArgsToResolve) {
    return new ClassNode(className, newArgs, newOptions, newOtherArgsToResolve);
  }

  public String getClassName() {
    return className;
  }
}
