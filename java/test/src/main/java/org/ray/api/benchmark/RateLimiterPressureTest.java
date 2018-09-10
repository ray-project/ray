package org.ray.api.benchmark;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.test.MyRunner;

@RunWith(MyRunner.class)
public class RateLimiterPressureTest extends RayBenchmarkTest {

  public static final int clientNum = 2;
  public static final int totalQps = 2;
  public static final int duration = 10;
  private static final long serialVersionUID = 6616958120966144235L;

  @RayRemote
  public static RemoteResult<Integer> currentTime() {
    RemoteResult<Integer> remoteResult = new RemoteResult<>();
    remoteResult.setFinishTime(System.nanoTime());
    remoteResult.setResult(0);
    return remoteResult;
  }

  @Test
  public void test() {
    PressureTestParameter pressureTestParameter = new PressureTestParameter();
    pressureTestParameter.setClientNum(clientNum);
    pressureTestParameter.setTotalQps(totalQps);
    pressureTestParameter.setDuration(duration);
    pressureTestParameter.setRayBenchmarkTest(this);
    super.rateLimiterPressureTest(pressureTestParameter);
  }

  @Override
  public RayObject<RemoteResult<Integer>> rayCall(RayActor rayActor) {

    return Ray.call(RateLimiterPressureTest::currentTime);
  }

  @Override
  public boolean checkResult(Object o) {
    return (int) o == 0;
  }
}
