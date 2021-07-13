package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.serializer.Hessian2Seserializer;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeReplicaTest {

  @SuppressWarnings("unused")
  @Test
  public void test() throws IOException {

    boolean inited = Ray.isInitialized();

    System.setProperty("ray.run-mode", "SINGLE_PROCESS");
    Ray.init();

    try {
      String controllerName = "RayServeReplicaTest";
      String backendTag = "b_tag";
      String replicaTag = "r_tag";

      ActorHandle<ReplicaContext> controllerHandle =
          Ray.actor(ReplicaContext::new, backendTag, replicaTag, controllerName, new Object())
              .setName(controllerName)
              .remote();

      BackendConfig.Builder backendConfig = BackendConfig.newBuilder();

      Object[] initArgs = new Object[] {backendTag, replicaTag, controllerName, new Object()};

      byte[] initArgsBytes = Hessian2Seserializer.encode(initArgs);

      ActorHandle<RayServeWrappedReplica> backendHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  backendTag,
                  replicaTag,
                  "io.ray.serve.ReplicaContext",
                  initArgsBytes,
                  backendConfig.build().toByteArray(),
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
