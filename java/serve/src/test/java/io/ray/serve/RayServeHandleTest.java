package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.DeploymentVersion;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeHandleTest {

  @Test
  public void test() {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String deploymentName = "RayServeHandleTest";
      String controllerName = deploymentName + "_controller";
      String replicaTag = deploymentName + "_replica";
      String actorName = replicaTag;
      String version = "v1";

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      // Replica
      DeploymentConfig.Builder deploymentConfigBuilder = DeploymentConfig.newBuilder();
      deploymentConfigBuilder.setDeploymentLanguage(DeploymentLanguage.JAVA);
      byte[] deploymentConfigBytes = deploymentConfigBuilder.build().toByteArray();

      Object[] initArgs = new Object[] {deploymentName, replicaTag, controllerName, new Object()};
      byte[] initArgsBytes = MessagePackSerializer.encode(initArgs).getLeft();

      DeploymentInfo deploymentInfo = new DeploymentInfo();
      deploymentInfo.setDeploymentConfig(deploymentConfigBytes);
      deploymentInfo.setDeploymentVersion(
          DeploymentVersion.newBuilder().setCodeVersion(version).build().toByteArray());
      deploymentInfo.setReplicaConfig(
          new ReplicaConfig("io.ray.serve.ReplicaContext", initArgsBytes, new HashMap<>()));

      ActorHandle<RayServeWrappedReplica> replicaHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  deploymentName,
                  replicaTag,
                  deploymentInfo,
                  controllerName)
              .setName(actorName)
              .remote();
      replicaHandle.task(RayServeWrappedReplica::ready).remote();

      // RayServeHandle
      RayServeHandle rayServeHandle =
          new RayServeHandle(controllerHandle, deploymentName, null, null)
              .setMethodName("getDeploymentName");
      ActorSet.Builder builder = ActorSet.newBuilder();
      builder.addNames(actorName);
      rayServeHandle.getRouter().getReplicaSet().updateWorkerReplicas(builder.build());

      // remote
      ObjectRef<Object> resultRef = rayServeHandle.remote(null);
      Assert.assertEquals((String) resultRef.get(), deploymentName);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
