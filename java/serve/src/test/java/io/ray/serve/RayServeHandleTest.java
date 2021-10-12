package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendLanguage;
import io.ray.serve.generated.BackendVersion;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeHandleTest {

  @Test
  public void test() {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String backendTag = "RayServeHandleTest";
      String controllerName = backendTag + "_controller";
      String replicaTag = backendTag + "_replica";
      String actorName = replicaTag;
      String version = "v1";

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      // Replica
      BackendConfig.Builder backendConfigBuilder = BackendConfig.newBuilder();
      backendConfigBuilder.setBackendLanguage(BackendLanguage.JAVA);
      byte[] backendConfigBytes = backendConfigBuilder.build().toByteArray();

      Object[] initArgs = new Object[] {backendTag, replicaTag, controllerName, new Object()};
      byte[] initArgsBytes = MessagePackSerializer.encode(initArgs).getLeft();

      DeploymentInfo deploymentInfo = new DeploymentInfo();
      deploymentInfo.setBackendConfig(backendConfigBytes);
      deploymentInfo.setBackendVersion(
          BackendVersion.newBuilder().setCodeVersion(version).build().toByteArray());
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

      // RayServeHandle
      RayServeHandle rayServeHandle =
          new RayServeHandle(controllerHandle, backendTag, null, null)
              .setMethodName("getBackendTag");
      ActorSet.Builder builder = ActorSet.newBuilder();
      builder.addNames(actorName);
      rayServeHandle.getRouter().getReplicaSet().updateWorkerReplicas(builder.build());

      // remote
      ObjectRef<Object> resultRef = rayServeHandle.remote(null);
      Assert.assertEquals((String) resultRef.get(), backendTag);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
