package io.ray.serve.handle;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.replica.RayServeWrappedReplica;
import io.ray.serve.replica.ReplicaContext;
import io.ray.serve.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeHandleTest {
  @Test
  public void test() {

    try {
      BaseServeTest.initRay();

      String deploymentName = "RayServeHandleTest";
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String replicaTag = deploymentName + "_replica";
      String actorName = replicaTag;
      String version = "v1";
      Map<String, String> config = new HashMap<>();
      config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new, "").setName(controllerName).remote();

      // Set ReplicaContext
      Serve.setInternalReplicaContext(null, null, controllerName, null, config);

      // Replica
      DeploymentConfig deploymentConfig =
          new DeploymentConfig().setDeploymentLanguage(DeploymentLanguage.JAVA);

      Object[] initArgs =
          new Object[] {deploymentName, replicaTag, controllerName, new Object(), new HashMap<>()};

      DeploymentWrapper deploymentWrapper =
          new DeploymentWrapper()
              .setName(deploymentName)
              .setDeploymentConfig(deploymentConfig)
              .setDeploymentVersion(new DeploymentVersion(version))
              .setDeploymentDef(ReplicaContext.class.getName())
              .setInitArgs(initArgs)
              .setConfig(config);

      ActorHandle<RayServeWrappedReplica> replicaHandle =
          Ray.actor(RayServeWrappedReplica::new, deploymentWrapper, replicaTag, controllerName)
              .setName(actorName)
              .remote();
      Assert.assertTrue(replicaHandle.task(RayServeWrappedReplica::checkHealth).remote().get());

      // RayServeHandle
      RayServeHandle rayServeHandle =
          new RayServeHandle(controllerHandle, deploymentName, null, null)
              .method("getDeploymentName");
      ActorNameList.Builder builder = ActorNameList.newBuilder();
      builder.addNames(actorName);
      rayServeHandle.getRouter().getReplicaSet().updateWorkerReplicas(builder.build());

      // remote
      ObjectRef<Object> resultRef = rayServeHandle.remote();
      Assert.assertEquals((String) resultRef.get(), deploymentName);
      Assert.assertTrue(rayServeHandle.isPolling());
    } finally {
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
