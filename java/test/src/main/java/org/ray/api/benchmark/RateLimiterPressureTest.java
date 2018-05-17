package org.ray.api.benchmark;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.test.MyRunner;

@RunWith(MyRunner.class)
public class RateLimiterPressureTest extends RayBenchmarkTest {

  private static final long serialVersionUID = 6616958120966144235L;

  public static final int clientNum = 2;

  public static final int totalQps = 2;

  public static final int duration = 10;

  @Test
  public void Test() {
    PressureTestParameter pressureTestParameter = new PressureTestParameter();
    pressureTestParameter.setClientNum(clientNum);
    pressureTestParameter.setTotalQps(totalQps);
    pressureTestParameter.setDuration(duration);
    pressureTestParameter.setRayBenchmarkTest(this);
    super.rateLimiterPressureTest(pressureTestParameter);
  }

  @RayRemote
  public static RemoteResult<Integer> currentTime() {
    RemoteResult<Integer> remoteResult = new RemoteResult<>();
    remoteResult.setFinishTime(System.nanoTime());
    remoteResult.setResult(0);
    return remoteResult;
  }

  @Override
  public boolean checkResult(Object o) {
    return (int) o == 0;
  }

  @Override
  public RayObject<RemoteResult<Integer>> rayCall(RayActor rayActor) {

    return Ray.call(RateLimiterPressureTest::currentTime);
  }
}
