package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendLanguage;
import io.ray.serve.generated.RequestMetadata;
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

      // Controller
      ActorHandle<ReplicaContext> controllerHandle =
          Ray.actor(ReplicaContext::new, backendTag, replicaTag, controllerName, new Object())
              .setName(controllerName)
              .remote();

      // Replica
      BackendConfig.Builder backendConfigBuilder = BackendConfig.newBuilder();
      backendConfigBuilder.setBackendLanguage(BackendLanguage.JAVA);
      byte[] backendConfigBytes = backendConfigBuilder.build().toByteArray();

      Object[] initArgs = new Object[] {backendTag, replicaTag, controllerName, new Object()};
      byte[] initArgsBytes = MessagePackSerializer.encode(initArgs).getLeft();

      ActorHandle<RayServeWrappedReplica> backendHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  backendTag,
                  replicaTag,
                  "io.ray.serve.ReplicaContext",
                  initArgsBytes,
                  backendConfigBytes,
                  controllerName)
              .setName(actorName)
              .remote();
      backendHandle.task(RayServeWrappedReplica::ready).remote();

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
