package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.DeploymentVersion;
import io.ray.serve.generated.RequestMetadata;
import java.util.HashMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RouterTest {

  @Test
  public void test() {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String backendTag = "RouterTest";
      String controllerName = backendTag + "_controller";
      String replicaTag = backendTag + "_replica";
      String actorName = replicaTag;
      String version = "v1";

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      // Replica
      DeploymentConfig.Builder deploymentConfigBuilder = DeploymentConfig.newBuilder();
      deploymentConfigBuilder.setDeploymentLanguage(DeploymentLanguage.JAVA);
      byte[] deploymentConfigBytes = deploymentConfigBuilder.build().toByteArray();

      Object[] initArgs = new Object[] {backendTag, replicaTag, controllerName, new Object()};
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
                  backendTag,
                  replicaTag,
                  deploymentInfo,
                  controllerName)
              .setName(actorName)
              .remote();
      replicaHandle.task(RayServeWrappedReplica::ready).remote();

      // Router
      Router router = new Router(controllerHandle, backendTag);
      ActorSet.Builder builder = ActorSet.newBuilder();
      builder.addNames(actorName);
      router.getReplicaSet().updateWorkerReplicas(builder.build());

      // assign
      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
      requestMetadata.setCallMethod("getBackendTag");

      ObjectRef<Object> resultRef = router.assignRequest(requestMetadata.build(), null);
      Assert.assertEquals((String) resultRef.get(), backendTag);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
