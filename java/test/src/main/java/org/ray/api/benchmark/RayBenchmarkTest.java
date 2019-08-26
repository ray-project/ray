package org.ray.api.benchmark;

import com.google.common.util.concurrent.RateLimiter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.function.RayFunc1;
import org.ray.api.test.BaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public abstract class RayBenchmarkTest<T> extends BaseTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayBenchmarkTest.class);
  //not thread safe ,but we only have one thread here
  public static final DecimalFormat df = new DecimalFormat("00.00");
  private static final long serialVersionUID = 416045641835782523L;

  @RayRemote
  private static List<Long> singleClient(PressureTestParameter pressureTestParameter) {

    try {
      List<Long> counterList = new ArrayList<>();
      PressureTestType pressureTestType = pressureTestParameter.getPressureTestType();
      RayBenchmarkTest rayBenchmarkTest = pressureTestParameter.getRayBenchmarkTest();
      int clientNum = pressureTestParameter.getClientNum();
      //SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      int len;
      String logPrefix;
      RateLimiter rateLimiter = null;
      if (pressureTestType.equals(PressureTestType.MAX)) {
        len = pressureTestParameter.getTotalNum() / clientNum;
        logPrefix = "MAX";
      } else {
        int totalQps = pressureTestParameter.getTotalQps();
        int duration = pressureTestParameter.getDuration();
        int qps = totalQps / clientNum;
        rateLimiter = RateLimiter.create(qps);
        len = qps * duration;
        logPrefix = "RATE_LIMITER";
      }
      RemoteResultWrapper[] remoteResultWrappers = new RemoteResultWrapper[len];
      int i = 0;
      while (i < len) {
        if (rateLimiter != null) {
          rateLimiter.acquire();
        }
        RemoteResultWrapper temp = new RemoteResultWrapper();
        temp.setStartTime(System.nanoTime());
        temp.setRayObject(rayBenchmarkTest.rayCall(pressureTestParameter.getRayActor()));
        remoteResultWrappers[i++] = temp;
      }

      int j = 0;
      while (j < len) {
        RemoteResultWrapper temp = remoteResultWrappers[j++];
        RemoteResult remoteResult = (RemoteResult) temp.getRayObject().get();
        long endTime = remoteResult.getFinishTime();
        long costTime = endTime - temp.getStartTime();
        counterList.add(costTime / 1000);
        LOGGER.warn("{}_cost_time:{}ns", logPrefix, costTime);
        Assert.assertTrue(rayBenchmarkTest.checkResult(remoteResult.getResult()));
      }
      return counterList;
    } catch (Exception e) {
      LOGGER.error("singleClient", e);
      return null;

    }
  }

  public void singleLatencyTest(int times, RayActor rayActor) {

    List<Long> counterList = new ArrayList<>();
    for (int i = 0; i < times; i++) {
      long startTime = System.nanoTime();
      RayObject<RemoteResult<T>> rayObject = rayCall(rayActor);
      RemoteResult<T> remoteResult = rayObject.get();
      T t = remoteResult.getResult();
      long endTime = System.nanoTime();
      long costTime = endTime - startTime;
      counterList.add(costTime / 1000);
      LOGGER.warn("SINGLE_LATENCY_cost_time: {} us", costTime);
      Assert.assertTrue(checkResult(t));
    }
    Collections.sort(counterList);
    printList(counterList);
  }

  public abstract RayObject<RemoteResult<T>> rayCall(RayActor rayActor);

  public abstract boolean checkResult(T t);

  private void printList(List<Long> list) {
    int len = list.size();
    int middle = len / 2;
    int almostHundred = (int) (len * 0.9999);
    int ninetyNine = (int) (len * 0.99);
    int ninetyFive = (int) (len * 0.95);
    int ninety = (int) (len * 0.9);
    int fifty = (int) (len * 0.5);

    LOGGER.error("Final result of rt as below:");
    LOGGER.error("max: {}μs", list.get(len - 1));
    LOGGER.error("min: {}μs", list.get(0));
    LOGGER.error("median: {}μs", list.get(middle));
    LOGGER.error("99.99% data smaller than: {}μs", list.get(almostHundred));
    LOGGER.error("99% data smaller than: {}μs", list.get(ninetyNine));
    LOGGER.error("95% data smaller than: {}μs", list.get(ninetyFive));
    LOGGER.error("90% data smaller than: {}μs", list.get(ninety));
    LOGGER.error("50% data smaller than: {}μs", list.get(fifty));
  }

  public void rateLimiterPressureTest(PressureTestParameter pressureTestParameter) {

    pressureTestParameter.setPressureTestType(PressureTestType.RATE_LIMITER);
    notSinglePressTest(pressureTestParameter);
  }

  private void notSinglePressTest(PressureTestParameter pressureTestParameter) {

    List<Long> counterList = new ArrayList<>();
    int clientNum = pressureTestParameter.getClientNum();
    RayObject<List<Long>>[] rayObjects = new RayObject[clientNum];

    for (int i = 0; i < clientNum; i++) {
      // Java compiler can't automatically infer the type of
      // `RayBenchmarkTest::singleClient`, because `RayBenchmarkTest` is a generic class.
      // It will match both `RayFunc1` and `RayFuncVoid1`. This looks like a bug or
      // defect of the Java compiler.
      // TODO(hchen): Figure out how to avoid manually declaring `RayFunc` type in this case.
      RayFunc1<PressureTestParameter, List<Long>> func = RayBenchmarkTest::singleClient;
      rayObjects[i] = Ray.call(func, pressureTestParameter);
    }
    for (int i = 0; i < clientNum; i++) {
      List<Long> subCounterList = rayObjects[i].get();
      Assert.assertNotNull(subCounterList);
      counterList.addAll(subCounterList);
    }
    Collections.sort(counterList);
    printList(counterList);
  }

  public void maxPressureTest(PressureTestParameter pressureTestParameter) {

    pressureTestParameter.setPressureTestType(PressureTestType.MAX);
    notSinglePressTest(pressureTestParameter);
  }

}
