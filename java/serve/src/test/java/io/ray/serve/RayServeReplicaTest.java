package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.DeploymentVersion;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.lang3.RandomStringUtils;
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
      String deploymentName = "b_tag";
      String replicaTag = "r_tag";
      String version = "v1";

      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

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

      ActorHandle<RayServeWrappedReplica> backendHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  deploymentName,
                  replicaTag,
                  deploymentInfo,
                  controllerName)
              .remote();

      // ready
      backendHandle.task(RayServeWrappedReplica::ready).remote();

      // handle request
      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
      requestMetadata.setCallMethod("getDeploymentName");
      RequestWrapper.Builder requestWrapper = RequestWrapper.newBuilder();

      ObjectRef<Object> resultRef =
          backendHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();
      Assert.assertEquals((String) resultRef.get(), deploymentName);

      // reconfigure
      ObjectRef<byte[]> versionRef =
          backendHandle.task(RayServeWrappedReplica::reconfigure, (Object) null).remote();
      Assert.assertEquals(DeploymentVersion.parseFrom(versionRef.get()).getCodeVersion(), version);

      // get version
      versionRef = backendHandle.task(RayServeWrappedReplica::getVersion).remote();
      Assert.assertEquals(DeploymentVersion.parseFrom(versionRef.get()).getCodeVersion(), version);

      // prepare for shutdown
      ObjectRef<Boolean> shutdownRef =
          backendHandle.task(RayServeWrappedReplica::prepareForShutdown).remote();
      Assert.assertTrue(shutdownRef.get());

    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
