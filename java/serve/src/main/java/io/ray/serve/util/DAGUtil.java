package io.ray.serve.util;

import io.ray.serve.dag.ClassNode;
import io.ray.serve.dag.DAGNode;
import java.util.Optional;

public class DAGUtil {

  public static String getNodeName(DAGNode node) {
    String nodeName = null;
    if (node instanceof ClassNode) {
      nodeName =
          Optional.ofNullable(node.getBoundOptions())
              .map(options -> (String) options.get("name"))
              .orElse(CommonUtil.getDeploymentName(((ClassNode) node).getClassName()));
    }
    return nodeName;
  }
}
