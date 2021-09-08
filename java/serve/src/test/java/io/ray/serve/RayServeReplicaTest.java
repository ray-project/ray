package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendLanguage;
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
      String backendTag = "b_tag";
      String replicaTag = "r_tag";

      ActorHandle<ReplicaContext> controllerHandle =
          Ray.actor(ReplicaContext::new, backendTag, replicaTag, controllerName, new Object())
              .setName(controllerName)
              .remote();

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
              .remote();

      backendHandle.task(RayServeWrappedReplica::ready).remote();

      RequestMetadata requestMetadata = new RequestMetadata();
      requestMetadata.setRequestId("RayServeReplicaTest");
      requestMetadata.setCallMethod("getBackendTag");
      ObjectRef<Object> resultRef =
          backendHandle
              .task(RayServeWrappedReplica::handleRequest, requestMetadata, (Object[]) null)
              .remote();

      Assert.assertEquals((String) resultRef.get(), backendTag);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
