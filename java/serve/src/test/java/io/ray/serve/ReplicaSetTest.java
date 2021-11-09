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
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaSetTest {

  private String deploymentName = "ReplicaSetTest";

  @Test
  public void setMaxConcurrentQueriesTest() {
    ReplicaSet replicaSet = new ReplicaSet(deploymentName);
    DeploymentConfig.Builder builder = DeploymentConfig.newBuilder();
    builder.setMaxConcurrentQueries(200);

    replicaSet.setMaxConcurrentQueries(builder.build());
    Assert.assertEquals(replicaSet.getMaxConcurrentQueries(), 200);
  }

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
    }
  }
}
