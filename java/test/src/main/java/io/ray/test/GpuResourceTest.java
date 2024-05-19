package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GpuResourceTest {
  private static class GpuActor {
    public List<Long> getGpuResourceFunc() {
      return Ray.getRuntimeContext().getGpuIds();
    }
  }

  @Test(groups = "cluster")
  public void testGetResourceId() {
    try {
      System.setProperty("ray.head-args.0", "--num-gpus=3");
      Ray.init();
      ActorHandle<GpuActor> gpuActorActorHandle =
          Ray.actor(GpuActor::new).setResource("GPU", 1D).remote();
      List<Long> gpuIds = gpuActorActorHandle.task(GpuActor::getGpuResourceFunc).remote().get();
      ActorHandle<GpuActor> gpuActorActorHandle2 =
          Ray.actor(GpuActor::new).setResource("GPU", 1D).remote();
      List<Long> gpuIds2 = gpuActorActorHandle2.task(GpuActor::getGpuResourceFunc).remote().get();
      ActorHandle<GpuActor> gpuActorActorHandle3 =
          Ray.actor(GpuActor::new).setResource("GPU", 0.5D).remote();
      List<Long> gpuIds3 = gpuActorActorHandle3.task(GpuActor::getGpuResourceFunc).remote().get();
      ActorHandle<GpuActor> gpuActorActorHandle4 =
          Ray.actor(GpuActor::new).setResource("GPU", 0.5D).remote();
      List<Long> gpuIds4 = gpuActorActorHandle4.task(GpuActor::getGpuResourceFunc).remote().get();
      Assert.assertNotEquals(gpuIds.get(0), gpuIds2.get(0));
      Assert.assertEquals(gpuIds3.get(0), gpuIds4.get(0));
    } finally {
      Ray.shutdown();
    }
  }
}
