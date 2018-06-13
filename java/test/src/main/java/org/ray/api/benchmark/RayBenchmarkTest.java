package org.ray.api.benchmark;

import com.google.common.util.concurrent.RateLimiter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.util.logger.RayLog;

public abstract class RayBenchmarkTest<T> implements Serializable {

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
        RayLog.core.warn(logPrefix + "_cost_time:" + costTime + "ns");
        Assert.assertTrue(rayBenchmarkTest.checkResult(remoteResult.getResult()));
      }
      return counterList;
    } catch (Exception e) {
      RayLog.core.error("singleClient", e);
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
      RayLog.core.warn("SINGLE_LATENCY_cost_time: " + costTime + " us");
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

    RayLog.core.error("Final result of rt as below:");
    RayLog.core.error("max: " + list.get(len - 1) + "μs");
    RayLog.core.error("min: " + list.get(0) + "μs");
    RayLog.core.error("median: " + list.get(middle) + "μs");
    RayLog.core.error("99.99% data smaller than: " + list.get(almostHundred) + "μs");
    RayLog.core.error("99% data smaller than: " + list.get(ninetyNine) + "μs");
    RayLog.core.error("95% data smaller than: " + list.get(ninetyFive) + "μs");
    RayLog.core.error("90% data smaller than: " + list.get(ninety) + "μs");
    RayLog.core.error("50% data smaller than: " + list.get(fifty) + "μs");
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
      rayObjects[i] = Ray.call(RayBenchmarkTest::singleClient, pressureTestParameter);
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
