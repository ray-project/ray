package io.ray.serve.dag;

import com.google.common.base.Preconditions;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.handle.DeploymentHandle;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.CommonUtil;
import io.ray.serve.util.DAGUtil;
import io.ray.serve.util.MessageFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class Graph {

  public static List<Deployment> build(DAGNode rayDagRootNode, String name) {
    DAGNodeBase serveRootDag =
        rayDagRootNode.applyRecursive(node -> transformRayDagToServeDag(node, name));
    List<Deployment> deployments = extractDeployments(serveRootDag);
    List<Deployment> deploymentsWithHttp = processIngressDeploymentInServeDag(deployments);
    return deploymentsWithHttp;
  }

  private static List<Deployment> processIngressDeploymentInServeDag(List<Deployment> deployments) {
    if (CollectionUtil.isEmpty(deployments)) {
      return deployments;
    }

    Deployment ingressDeployment = deployments.get(deployments.size() - 1);
    if (StringUtils.isBlank(ingressDeployment.getRoutePrefix())
        || StringUtils.equals(
            ingressDeployment.getRoutePrefix(), "/" + ingressDeployment.getName())) {
      ingressDeployment.setRoutePrefix("/");
    }

    for (int i = 0; i < deployments.size() - 1; i++) {
      Deployment deployment = deployments.get(i);
      Preconditions.checkArgument(
          StringUtils.isBlank(deployment.getRoutePrefix())
              || StringUtils.equals(deployment.getRoutePrefix(), "/" + deployment.getName()),
          MessageFormatter.format(
              "Route prefix is only configurable on the ingress deployment. "
                  + "Please do not set non-default route prefix: "
                  + "{} on non-ingress deployment of the "
                  + "serve DAG. ",
              deployment.getRoutePrefix()));
      deployment.setRoutePrefix(null);
    }
    return deployments;
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

      String routePrefix =
          StringUtils.isBlank(deploymentShell.getRoutePrefix())
                  || !StringUtils.equals(deploymentShell.getRoutePrefix(), "/" + deploymentName)
              ? deploymentShell.getRoutePrefix()
              : "/" + deploymentName;

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
              .setRoutePrefix(routePrefix)
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

  public static Deployment getAndValidateIngressDeployment(List<Deployment> deployments) {

    List<Deployment> ingressDeployments = new ArrayList<>();
    for (Deployment deployment : deployments) {
      if (StringUtils.isNotBlank(deployment.getRoutePrefix())) {
        ingressDeployments.add(deployment);
      }
    }

    Preconditions.checkArgument(
        ingressDeployments.size() == 1,
        MessageFormatter.format(
            "Only one deployment in an Serve Application or DAG can have non-None route prefix. {} ingress deployments found: {}",
            ingressDeployments.size(),
            ingressDeployments));

    ingressDeployments.get(0).setIngress(true);
    return ingressDeployments.get(0);
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
