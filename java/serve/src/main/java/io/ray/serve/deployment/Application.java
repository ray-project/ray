package io.ray.serve.deployment;

import io.ray.serve.api.Serve;
import io.ray.serve.handle.RayServeHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Application {

  private List<Deployment> deployments;
  public Application(Deployment deployment) {
    List<Deployment> deploymentList = new ArrayList<>();
    deploymentList.add(deployment);
    this.deployments = deploymentList;
  }
//  public Deployment getDeployment() {
//    if (deployments.size() > 0) {
//      return deployments.get(0);
//    }
//    else return null;
//  }
  public List<Deployment> getDeployments() {
    return deployments;
  }


  public void deployApplication() {
    List<Deployment> deployments  = this.getDeployments();
    List< HashMap<String,Object> > parameter_group = new ArrayList<>();
    for (Deployment deployment : deployments) {
        HashMap<String, Object> deployment_parameters = new HashMap<String, Object>() {
            {
    //              String deploymentDef,
    //              String name,
    //              DeploymentConfig config,
    //              String version,
    //              String prevVersion,
    //              Object[] initArgs,
    //              String routePrefix,
              put("name", deployment.getName());
              put("deploymentDef", deployment.getDeploymentDef());
              put("config", deployment.getConfig());
              put("version", deployment.getVersion());
              put("prevVersion", deployment.getPrevVersion());
              put("initArgs", deployment.getInitArgs());
              put("routePrefix", deployment.getRoutePrefix());
              put("rayActorOptions", deployment.getRayActorOptions());
            }
        };
        parameter_group.add(deployment_parameters);
    }
//    Serve.getGlobalClient()
//      .deployApplication(
//        parameter_group,
//        true);
    Deployment deployment = deployments.get(0);
    deployment.deploy(true);
  }

  public RayServeHandle getHandle() {
      List<Deployment> deployments  = this.getDeployments();
      Deployment deployment = deployments.get(0);
      return deployment.getHandle();
  }
}