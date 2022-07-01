package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeReplicaTest {

  @SuppressWarnings("unused")
  @Test
  public void test() throws IOException {
    boolean inited = Ray.isInitialized();
    String previous_namespace = System.getProperty("ray.job.namespace");
    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
    Ray.init();

    try {
      String controllerName = "RayServeReplicaTest";
      String deploymentName = "b_tag";
      String replicaTag = "r_tag";
      String version = "v1";

      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      DeploymentConfig deploymentConfig =
          new DeploymentConfig().setDeploymentLanguage(DeploymentLanguage.JAVA.getNumber());
      DeploymentInfo deploymentInfo =
          new DeploymentInfo()
              .setName(deploymentName)
              .setDeploymentConfig(deploymentConfig)
              .setDeploymentVersion(new DeploymentVersion(version))
              .setDeploymentDef(DummyReplica.class.getName());

      ActorHandle<RayServeWrappedReplica> replicHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  deploymentInfo,
                  replicaTag,
                  controllerName,
                  new RayServeConfig().setConfig(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false"))
              .remote();

      // ready
      Assert.assertTrue(replicHandle.task(RayServeWrappedReplica::checkHealth).remote().get());

      // handle request
      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
      requestMetadata.setCallMethod(Constants.CALL_METHOD);
      RequestWrapper.Builder requestWrapper = RequestWrapper.newBuilder();

      ObjectRef<Object> resultRef =
          replicHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();
      Assert.assertEquals((String) resultRef.get(), "1");

      // reconfigure
      ObjectRef<Object> versionRef =
          replicHandle.task(RayServeWrappedReplica::reconfigure, (Object) null).remote();
      Assert.assertEquals(((DeploymentVersion) versionRef.get()).getCodeVersion(), version);

      replicHandle.task(RayServeWrappedReplica::reconfigure, new Object()).remote().get();
      resultRef =
          replicHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();
      Assert.assertEquals((String) resultRef.get(), "1");

      replicHandle
          .task(RayServeWrappedReplica::reconfigure, ImmutableMap.of("value", "100"))
          .remote()
          .get();
      resultRef =
          replicHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();
      Assert.assertEquals((String) resultRef.get(), "101");

      // get version
      versionRef = replicHandle.task(RayServeWrappedReplica::getVersion).remote();
      Assert.assertEquals(((DeploymentVersion) versionRef.get()).getCodeVersion(), version);

      // prepare for shutdown
      ObjectRef<Boolean> shutdownRef =
          replicHandle.task(RayServeWrappedReplica::prepareForShutdown).remote();
      Assert.assertTrue(shutdownRef.get());

    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      if (previous_namespace == null) {
        System.clearProperty("ray.job.namespace");
      } else {
        System.setProperty("ray.job.namespace", previous_namespace);
      }
      Serve.setInternalReplicaContext(null);
    }
  }
}
