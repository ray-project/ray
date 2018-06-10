package org.ray.api.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayList;
import org.ray.api.RayMap;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.returns.MultipleReturns2;
import org.ray.api.returns.RayObjects2;

/**
 * types test.
 */
@RunWith(MyRunner.class)
public class TypesTest {

  @RayRemote
  public static int sayInt() {
    return 1;
  }

  @RayRemote
  public static byte sayByte() {
    return 1;
  }

  @RayRemote
  public static short sayShort() {
    return 1;
  }

  @RayRemote
  public static long sayLong() {
    return 1;
  }

  @RayRemote
  public static double sayDouble() {
    return 1;
  }

  @RayRemote
  public static float sayFloat() {
    return 1;
  }

  @RayRemote
  public static boolean sayBool() {
    return true;
  }

  @RayRemote
  public static Object sayReference() {
    return "object";
  }

  @RayRemote
  public static MultipleReturns2<Integer, String> sayReferences() {
    return new MultipleReturns2<>(123, "123");
  }

  @RayRemote
  public static Map<Integer, String> sayReferencesN(Collection<Integer> userReturnIds,
                                                    String prefix, String suffix) {
    Map<Integer, String> ret = new HashMap<>();
    for (Integer returnid : userReturnIds) {
      ret.put(returnid, prefix + returnid + suffix);
    }
    return ret;
  }

  @RayRemote
  public static List<Integer> sayArray(Integer returnCount) {
    ArrayList<Integer> rets = new ArrayList<>();
    for (int i = 0; i < returnCount; i++) {
      rets.add(i);
    }
    return rets;
  }

  @RayRemote(externalIo = true)
  public static Integer sayRayFuture() {
    return 123;
  }

  @RayRemote(externalIo = true)
  public static MultipleReturns2<Integer, String> sayRayFutures() {
    return new MultipleReturns2<>(123, "123");
  }

  @RayRemote(externalIo = true)
  public static Map<Integer, String> sayRayFuturesN(
      Collection<Integer/*user's custom return_id*/> userReturnIds,
      String prefix) {
    Map<Integer, String> ret = new HashMap<>();
    for (int id : userReturnIds) {
      ret.put(id, prefix + id);
    }
    return ret;
  }

  @RayRemote
  public static int sayReadRayList(List<Integer> ints) {
    int sum = 0;
    for (Integer i : ints) {
      sum += i;
    }
    return sum;
  }

  @RayRemote
  public static int sayReadRayMap(Map<String, Integer> ints) {
    int sum = 0;
    for (Integer i : ints.values()) {
      sum += i;
    }
    return sum;
  }

  @Test
  public void test() {
    sayTypes();
  }

  public void sayTypes() {

    Assert.assertEquals(1, (int) Ray.call(TypesTest::sayInt).get());
    Assert.assertEquals(1, (byte) Ray.call(TypesTest::sayByte).get());
    Assert.assertEquals(1, (short) Ray.call(TypesTest::sayShort).get());
    Assert.assertEquals(1, (long) Ray.call(TypesTest::sayLong).get());
    Assert.assertEquals(1.0, Ray.call(TypesTest::sayDouble).get(), 0.0);
    Assert.assertEquals(1.0f, Ray.call(TypesTest::sayFloat).get(), 0.0);
    Assert.assertEquals(true, Ray.call(TypesTest::sayBool).get());
    Assert.assertEquals("object", Ray.call(TypesTest::sayReference).get());

    RayObjects2<Integer, String> refs = Ray.call_2(TypesTest::sayReferences);
    Assert.assertEquals(123, (int) refs.r0().get());
    Assert.assertEquals("123", refs.r1().get());

    RayMap<Integer, String> futureRs = Ray.call_n(TypesTest::sayReferencesN,
        Arrays.asList(1, 2, 4, 3), "n_refs_", "_suffix");
    for (Entry<Integer, RayObject<String>> fne : futureRs.EntrySet()) {
      Assert.assertEquals(fne.getValue().get(), "n_refs_" + fne.getKey() + "_suffix");
    }

    RayMap<Integer, String> futureRs2 = Ray.call_n(TypesTest::sayReferencesN,
        Arrays.asList(1), "n_refs_", "_suffix");
    for (Entry<Integer, RayObject<String>> fne : futureRs2.EntrySet()) {
      Assert.assertEquals(fne.getValue().get(), "n_refs_" + fne.getKey() + "_suffix");
    }

    RayObject<Integer> future = Ray.call(TypesTest::sayRayFuture);
    Assert.assertEquals(123, (int) future.get());
    RayObjects2<Integer, String> futures = Ray.call_2(TypesTest::sayRayFutures);
    Assert.assertEquals(123, (int) futures.r0().get());
    Assert.assertEquals("123", futures.r1().get());
    RayMap<Integer, String> futureNs = Ray.call_n(TypesTest::sayRayFuturesN,
        Arrays.asList(1, 2, 4, 3), "n_futures_");
    for (Entry<Integer, RayObject<String>> fne : futureNs.EntrySet()) {
      Assert.assertEquals(fne.getValue().get(), "n_futures_" + fne.getKey());
    }

    RayList<Integer> ns = Ray.call_n(TypesTest::sayArray, 10, 10);
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(i, (int) ns.Get(i).get());
    }

    RayList<Integer> ns2 = Ray.call_n(TypesTest::sayArray, 1, 1);
    Assert.assertEquals(0, (int) ns2.Get(0).get());

    RayObject<List<Integer>> ns3 = Ray.call(TypesTest::sayArray, 1);
    Assert.assertEquals(0, (int) ns3.get().get(0));

    RayList<Integer> ints = new RayList<>();
    ints.add(Ray.call(TypesTest::sayInt));
    ints.add(Ray.call(TypesTest::sayInt));
    ints.add(Ray.call(TypesTest::sayInt));
    // TODO: when RayParameters.use_remote_lambda is on, we have to explicitly
    // cast RayList and RayMap to List and map explicitly, so that the parameter
    // types of the lambdas can be correctly deducted.
    RayObject<Integer> collection = Ray.call(TypesTest::sayReadRayList, (List<Integer>) ints);
    Assert.assertEquals(3, (int) collection.get());

    RayMap<String, Integer> namedInts = new RayMap();
    namedInts.put("a", Ray.call(TypesTest::sayInt));
    namedInts.put("b", Ray.call(TypesTest::sayInt));
    namedInts.put("c", Ray.call(TypesTest::sayInt));
    RayObject<Integer> collection2 = Ray
        .call(TypesTest::sayReadRayMap, (Map<String, Integer>) namedInts);
    Assert.assertEquals(3, (int) collection2.get());
  }
}
