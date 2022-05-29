package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.controller.ControllerInfo;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.replica.RayServeWrappedReplica;
import io.ray.serve.router.Router;
import io.ray.serve.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RouterTest {

  @Test
  public void test() {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String deploymentName = "RouterTest";
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
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      // Replica
      DeploymentConfig deploymentConfig =
          new DeploymentConfig().setDeploymentLanguage(DeploymentLanguage.JAVA);

      Object[] initArgs = new Object[] {deploymentName, replicaTag, controllerName, new Object()};

      DeploymentWrapper deploymentWrapper =
          new DeploymentWrapper()
              .setName(deploymentName)
              .setDeploymentConfig(deploymentConfig)
              .setDeploymentVersion(new DeploymentVersion(version))
              .setDeploymentDef("io.ray.serve.ReplicaContext")
              .setInitArgs(initArgs);

      ActorHandle<RayServeWrappedReplica> replicaHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  deploymentWrapper,
                  replicaTag,
                  new ControllerInfo(controllerName, null))
              .setName(actorName)
              .remote();
      Assert.assertTrue(replicaHandle.task(RayServeWrappedReplica::checkHealth).remote().get());

      // Set ReplicaContext
      Serve.setInternalReplicaContext(null, null, controllerName, null, null, config);

      // Router
      Router router = new Router(controllerHandle, deploymentName);
      ActorSet.Builder builder = ActorSet.newBuilder();
      builder.addNames(actorName);
      router.getReplicaSet().updateWorkerReplicas(builder.build());

      // assign
      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
      requestMetadata.setCallMethod("getDeploymentName");

      ObjectRef<Object> resultRef = router.assignRequest(requestMetadata.build(), null);
      Assert.assertEquals((String) resultRef.get(), deploymentName);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      Serve.setInternalReplicaContext(null);
    }
  }
}
