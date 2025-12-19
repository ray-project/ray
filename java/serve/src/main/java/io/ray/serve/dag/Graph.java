package io.ray.serve.dag;

import io.ray.serve.deployment.Deployment;
import io.ray.serve.handle.DeploymentHandle;
import io.ray.serve.util.CommonUtil;
import io.ray.serve.util.DAGUtil;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class Graph {

  public static List<Deployment> build(DAGNode rayDagRootNode, String name) {
    DAGNodeBase serveRootDag =
        rayDagRootNode.applyRecursive(node -> transformRayDagToServeDag(node, name));
    return extractDeployments(serveRootDag);
  }

  public static DAGNodeBase transformRayDagToServeDag(DAGNodeBase dagNode, String appName) {
    if (dagNode instanceof ClassNode) {
      ClassNode clsNode = (ClassNode) dagNode;
      Deployment deploymentShell =
          (Deployment) clsNode.getBoundOtherArgsToResolve().get("deployment_schema");

      String deploymentName = DAGUtil.getNodeName(clsNode);
      if (!StringUtils.equals(
          deploymentShell.getName(), CommonUtil.getDeploymentName(clsNode.getClassName()))) {
        deploymentName = deploymentShell.getName();
      }

      Object[] replacedDeploymentInitArgs = new Object[clsNode.getBoundArgs().length];
      for (int i = 0; i < clsNode.getBoundArgs().length; i++) {
        replacedDeploymentInitArgs[i] =
            clsNode.getBoundArgs()[i] instanceof DeploymentNode
                ? replaceWithHandle((DeploymentNode) clsNode.getBoundArgs()[i])
                : clsNode.getBoundArgs()[i];
      }

      Deployment deployment =
          deploymentShell
              .options()
              .setDeploymentDef(clsNode.getClassName())
              .setName(deploymentName)
              .setInitArgs(replacedDeploymentInitArgs)
              .create(false);

      return new DeploymentNode(
          deployment,
          appName,
          clsNode.getBoundArgs(),
          clsNode.getBoundOptions(),
          clsNode.getBoundOtherArgsToResolve());
    }

    return dagNode;
  }

  public static List<Deployment> extractDeployments(DAGNodeBase rootNode) {
    Map<String, Deployment> deployments = new LinkedHashMap<>();
    rootNode.applyRecursive(
        node -> {
          if (node instanceof DeploymentNode) {
            Deployment deployment = ((DeploymentNode) node).getDeployment();
            deployments.put(deployment.getName(), deployment);
          }
          return node;
        });
    return deployments.values().stream().collect(Collectors.toList());
  }

  public static DeploymentHandle replaceWithHandle(DAGNode node) {
    if (node instanceof DeploymentNode) {
      DeploymentNode deploymentNode = (DeploymentNode) node;
      return new DeploymentHandle(
          deploymentNode.getDeployment().getName(), deploymentNode.getAppName());
    }
    return null;
  }
}
