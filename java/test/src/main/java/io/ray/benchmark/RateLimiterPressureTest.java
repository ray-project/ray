package io.ray.benchmark;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.annotations.Test;

public class RateLimiterPressureTest extends RayBenchmarkTest {

  public static final int clientNum = 2;
  public static final int totalQps = 2;
  public static final int duration = 10;
  private static final long serialVersionUID = 6616958120966144235L;

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
  public ObjectRef<RemoteResult<Integer>> rayCall(ActorHandle rayActor) {

    return Ray.task(RateLimiterPressureTest::currentTime).remote();
  }

  @Override
  public boolean checkResult(Object o) {
    return (int) o == 0;
  }
}
