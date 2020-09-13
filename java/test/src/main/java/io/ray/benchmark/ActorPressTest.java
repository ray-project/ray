package io.ray.benchmark;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.annotations.Test;

public class ActorPressTest extends RayBenchmarkTest {

  @Test
  public void singleLatencyTest() {
    int times = 10;
    ActorHandle<Adder> adder = Ray.actor(ActorPressTest.Adder::new).remote();
    super.singleLatencyTest(times, adder);
  }

  @Test
  public void maxTest() {
    int clientNum = 2;
    int totalNum = 20;
    ActorHandle<Adder> adder = Ray.actor(ActorPressTest.Adder::new).remote();
    PressureTestParameter pressureTestParameter = new PressureTestParameter();
    pressureTestParameter.setClientNum(clientNum);
    pressureTestParameter.setTotalNum(totalNum);
    pressureTestParameter.setRayBenchmarkTest(this);
    pressureTestParameter.setRayActor(adder);
    super.maxPressureTest(pressureTestParameter);
  }

  @Test
  public void rateLimiterTest() {
    int clientNum = 2;
    int totalQps = 2;
    int duration = 3;
    ActorHandle<Adder> adder = Ray.actor(ActorPressTest.Adder::new).remote();
    PressureTestParameter pressureTestParameter = new PressureTestParameter();
    pressureTestParameter.setClientNum(clientNum);
    pressureTestParameter.setTotalQps(totalQps);
    pressureTestParameter.setDuration(duration);
    pressureTestParameter.setRayBenchmarkTest(this);
    pressureTestParameter.setRayActor(adder);
    super.rateLimiterPressureTest(pressureTestParameter);
  }

  @Override
  public ObjectRef<RemoteResult<Integer>> rayCall(ActorHandle rayActor) {
    return ((ActorHandle<Adder>) rayActor).task(Adder::add, 10).remote();
  }

  @Override
  public boolean checkResult(Object o) {
    return true;
  }

  public static class Adder {

    private Integer sum = 0;

    public RemoteResult<Integer> add(Integer n) {
      RemoteResult<Integer> remoteResult = new RemoteResult<>();
      remoteResult.setResult(sum += n);
      remoteResult.setFinishTime(System.nanoTime());
      return remoteResult;
    }
  }

}
