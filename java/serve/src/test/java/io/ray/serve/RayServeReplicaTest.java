package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.poll.KeyType;
import io.ray.serve.poll.LongPollNamespace;
import io.ray.serve.poll.UpdatedObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeReplicaTest {

  @Test
  public void test() {

    System.setProperty("ray.run-mode", "SINGLE_PROCESS");
    Ray.init();

    String controllerName = "RayServeReplicaTest";
    String backendTag = "b_tag";
    String replicaTag = "r_tag";

    ActorHandle<DummyController> controllerHandle =
        Ray.actor(DummyController::new).setName(controllerName).remote();

    BackendConfig backendConfig = new BackendConfig();
    backendConfig.setUserConfig(10);
    ActorHandle<RayServeWrappedReplica> backendHandle = Ray.actor(RayServeWrappedReplica::new,
        backendTag, replicaTag, "io.ray.serve.RayServeReplicaTest.DummyBackend", new Object[] {0},
        backendConfig, controllerName).remote();

    backendHandle.task(RayServeWrappedReplica::ready).remote();

    RequestMetadata requestMetadata = new RequestMetadata();
    requestMetadata.setRequestId("RayServeReplicaTest");
    ObjectRef<Object> resultRef = backendHandle
        .task(RayServeWrappedReplica::handle_request, requestMetadata, (Object[]) null).remote();

    Assert.assertEquals(((Integer) resultRef.get()).intValue(), 10);

  }

  static class DummyController {

    public Map<KeyType, UpdatedObject> listen_for_change(Map<KeyType, Integer> snapshotIds) {
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
      }
      UpdatedObject updatedObject = new UpdatedObject();
      updatedObject.setSnapshotId(2);
      updatedObject.setObjectSnapshot(new BackendConfig());
      Map<KeyType, UpdatedObject> updates = new HashMap<>();
      updates.put(new KeyType(LongPollNamespace.BACKEND_CONFIGS, "b_tag"),
          updatedObject);
      return updates;
    }
  }

  static class DummyBackend {

    private AtomicInteger counter;

    public DummyBackend(Integer value) {
      counter = new AtomicInteger(value);
    }

    public Integer __call__() {
      return counter.get();
    }

    public void reconfigure(Integer value) {
      if (value == null) {
        return;
      }
      counter.addAndGet(value);
    }
  }


}
