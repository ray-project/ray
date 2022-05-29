package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.controller.ControllerInfo;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.replica.RayServeWrappedReplica;
import io.ray.serve.router.Query;
import io.ray.serve.router.ReplicaSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaSetTest {

  private String deploymentName = "ReplicaSetTest";

  @Test
  public void updateWorkerReplicasTest() {
    ReplicaSet replicaSet = new ReplicaSet(deploymentName);
    ActorSet.Builder builder = ActorSet.newBuilder();

    replicaSet.updateWorkerReplicas(builder.build());
    Map<ActorHandle<RayServeWrappedReplica>, Set<ObjectRef<Object>>> inFlightQueries =
        replicaSet.getInFlightQueries();
    Assert.assertTrue(inFlightQueries.isEmpty());
  }

  @SuppressWarnings("unused")
  @Test
  public void assignReplicaTest() {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String controllerName = deploymentName + "_controller";
      String replicaTag = deploymentName + "_replica";
      String actorName = replicaTag;
      String version = "v1";

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

      // ReplicaSet
      ReplicaSet replicaSet = new ReplicaSet(deploymentName);
      ActorSet.Builder builder = ActorSet.newBuilder();
      builder.addNames(actorName);
      replicaSet.updateWorkerReplicas(builder.build());

      // assign
      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
      requestMetadata.setCallMethod("getDeploymentName");

      Query query = new Query(requestMetadata.build(), null);
      ObjectRef<Object> resultRef = replicaSet.assignReplica(query);

      Assert.assertEquals((String) resultRef.get(), deploymentName);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      Serve.setInternalReplicaContext(null);
    }
  }
}
