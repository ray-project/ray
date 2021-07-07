package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeReplicaTest {

  @SuppressWarnings("unused")
  @Test
  public void test() {

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

      BackendConfig backendConfig = new BackendConfig();
      ActorHandle<RayServeWrappedReplica> backendHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  backendTag,
                  replicaTag,
                  "io.ray.serve.ReplicaContext",
                  new Object[] {backendTag, replicaTag, controllerName, new Object()},
                  backendConfig,
                  controllerName)
              .remote();

      backendHandle.task(RayServeWrappedReplica::ready).remote();

      RequestMetadata requestMetadata = new RequestMetadata();
      requestMetadata.setRequestId("RayServeReplicaTest");
      requestMetadata.setCallMethod("getBackendTag");
      ObjectRef<Object> resultRef =
          backendHandle
              .task(RayServeWrappedReplica::handle_request, requestMetadata, (Object[]) null)
              .remote();

      Assert.assertEquals((String) resultRef.get(), backendTag);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
