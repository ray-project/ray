package io.ray.serve.router;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.replica.RayServeWrappedReplica;
import io.ray.serve.replica.ReplicaContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaSetTest {
  private String deploymentName = "ReplicaSetTest";

  @Test
  public void updateWorkerReplicasTest() {
    try {
      BaseServeTest.initRay();
      ReplicaSet replicaSet = new ReplicaSet(deploymentName);
      ActorNameList.Builder builder = ActorNameList.newBuilder();

      replicaSet.updateWorkerReplicas(builder.build());
      Map<String, Set<ObjectRef<Object>>> inFlightQueries = replicaSet.getInFlightQueries();
      Assert.assertTrue(inFlightQueries.isEmpty());
    } finally {
      BaseServeTest.shutdownRay();
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void assignReplicaTest() {
    try {
      BaseServeTest.initRay();

      String controllerName = deploymentName + "_controller";
      String replicaTag = deploymentName + "_replica";
      String actorName = replicaTag;
      String version = "v1";

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new, "").setName(controllerName).remote();

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
              .setInitArgs(initArgs);

      ActorHandle<RayServeWrappedReplica> replicaHandle =
          Ray.actor(RayServeWrappedReplica::new, deploymentWrapper, replicaTag, controllerName)
              .setName(actorName)
              .remote();
      Assert.assertTrue(replicaHandle.task(RayServeWrappedReplica::checkHealth).remote().get());

      // ReplicaSet
      ReplicaSet replicaSet = new ReplicaSet(deploymentName);
      ActorNameList.Builder builder = ActorNameList.newBuilder();
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
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
