package io.ray.serve.replica;

import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.util.CommonUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeReplicaTest {
  @Test
  public void test() throws IOException {

    try {
      BaseServeTest.initRay();

      String prefix = "RayServeReplicaTest";
      String controllerName = CommonUtil.formatActorName(Constants.SERVE_CONTROLLER_NAME, prefix);
      String deploymentName = prefix + "_deployment";
      String replicaTag = prefix + "_replica";
      String version = "v1";
      Map<String, String> config = new HashMap<>();
      config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

      Ray.actor(DummyServeController::new, "").setName(controllerName).remote();

      DeploymentConfig deploymentConfig =
          new DeploymentConfig().setDeploymentLanguage(DeploymentLanguage.JAVA);
      DeploymentWrapper deploymentWrapper =
          new DeploymentWrapper()
              .setName(deploymentName)
              .setDeploymentConfig(deploymentConfig)
              .setDeploymentVersion(new DeploymentVersion(version))
              .setDeploymentDef(DummyReplica.class.getName());

      ActorHandle<RayServeWrappedReplica> replicHandle =
          Ray.actor(RayServeWrappedReplica::new, deploymentWrapper, replicaTag, controllerName)
              .remote();

      // ready
      Assert.assertTrue(replicHandle.task(RayServeWrappedReplica::checkHealth).remote().get());

      // handle request
      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
      requestMetadata.setCallMethod(Constants.CALL_METHOD);

      ObjectRef<Object> resultRef =
          replicHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  new Object[0])
              .remote();
      Assert.assertEquals((String) resultRef.get(), "1");

      // reconfigure
      ObjectRef<Object> versionRef =
          replicHandle
              .task(RayServeWrappedReplica::reconfigure, (new DeploymentConfig()).toProtoBytes())
              .remote();
      Assert.assertEquals(
          DeploymentVersion.fromProtoBytes((byte[]) (versionRef.get())).getCodeVersion(), version);

      deploymentConfig = deploymentConfig.setUserConfig(new Object());
      replicHandle
          .task(RayServeWrappedReplica::reconfigure, deploymentConfig.toProtoBytes())
          .remote()
          .get();
      resultRef =
          replicHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  new Object[0])
              .remote();
      Assert.assertEquals((String) resultRef.get(), "1");

      deploymentConfig = deploymentConfig.setUserConfig(ImmutableMap.of("value", "100"));
      replicHandle
          .task(RayServeWrappedReplica::reconfigure, deploymentConfig.toProtoBytes())
          .remote()
          .get();
      resultRef =
          replicHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  new Object[0])
              .remote();
      Assert.assertEquals((String) resultRef.get(), "101");

      // get version
      versionRef = replicHandle.task(RayServeWrappedReplica::getVersion).remote();
      Assert.assertEquals(
          DeploymentVersion.fromProtoBytes((byte[]) (versionRef.get())).getCodeVersion(), version);

      // prepare for shutdown
      ObjectRef<Boolean> shutdownRef =
          replicHandle.task(RayServeWrappedReplica::prepareForShutdown).remote();
      Assert.assertTrue(shutdownRef.get());

    } finally {
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
