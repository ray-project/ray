package org.ray.api.benchmark;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.test.MyRunner;

@RunWith(MyRunner.class)
public class ActorPressTest extends RayBenchmarkTest {

  @Test
  public void singleLatencyTest() {
    int times = 10;
    RayActor<ActorPressTest.Adder> adder = Ray.createActor(ActorPressTest.Adder.class);
    super.singleLatencyTest(times, adder);
  }

  @Test
  public void maxTest() {
    int clientNum = 2;
    int totalNum = 20;
    RayActor<ActorPressTest.Adder> adder = Ray.createActor(ActorPressTest.Adder.class);
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
    RayActor<ActorPressTest.Adder> adder = Ray.createActor(ActorPressTest.Adder.class);
    PressureTestParameter pressureTestParameter = new PressureTestParameter();
    pressureTestParameter.setClientNum(clientNum);
    pressureTestParameter.setTotalQps(totalQps);
    pressureTestParameter.setDuration(duration);
    pressureTestParameter.setRayBenchmarkTest(this);
    pressureTestParameter.setRayActor(adder);
    super.rateLimiterPressureTest(pressureTestParameter);
  }

  @Override
  public RayObject<RemoteResult<Integer>> rayCall(RayActor rayActor) {
    return Ray.call(Adder::add, (RayActor<Adder>) rayActor, 10);
  }

  @Override
  public boolean checkResult(Object o) {
    return true;
  }

  @RayRemote
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
