package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeReplicaTest {

  @SuppressWarnings("unused")
  @Test
  public void test() throws IOException {

    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String controllerName = "RayServeReplicaTest";
      String DeploymentTag = "d_tag";
      String replicaTag = "r_tag";

      ActorHandle<ReplicaContext> controllerHandle =
          Ray.actor(ReplicaContext::new, DeploymentTag, replicaTag, controllerName, new Object())
              .setName(controllerName)
              .remote();

      DeploymentConfig.Builder DeploymentConfigBuilder = DeploymentConfig.newBuilder();
      DeploymentConfigBuilder.setDeploymentLanguage(DeploymentLanguage.JAVA);

      byte[] DeploymentConfigBytes = DeploymentConfigBuilder.build().toByteArray();

      Object[] initArgs = new Object[] {DeploymentTag, replicaTag, controllerName, new Object()};

      byte[] initArgsBytes = MessagePackSerializer.encode(initArgs).getLeft();

      ActorHandle<RayServeWrappedReplica> deploymentHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  DeploymentTag,
                  replicaTag,
                  "io.ray.serve.ReplicaContext",
                  initArgsBytes,
                  DeploymentConfigBytes,
                  controllerName)
              .remote();

      deploymentHandle.task(RayServeWrappedReplica::ready).remote();

      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId("RayServeReplicaTest");
      requestMetadata.setCallMethod("getDeploymentTag");

      RequestWrapper.Builder requestWrapper = RequestWrapper.newBuilder();

      ObjectRef<Object> resultRef =
          deploymentHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();

      Assert.assertEquals((String) resultRef.get(), DeploymentTag);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
