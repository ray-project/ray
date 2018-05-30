package org.ray.api.test;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.UniqueID;

@RunWith(MyRunner.class)
public class ActorTest {

  @Test
  public void Test() {

    RayActor<ActorTest.Adder> adder = Ray.create(ActorTest.Adder.class);
    Ray.call(Adder::set, adder, 10);
    RayObject<Integer> result = Ray.call(Adder::add, adder, 1);
    Assert.assertEquals(11, (int) result.get());

    RayActor<Adder> secondAdder = Ray.create(Adder.class);
    RayObject<Integer> result2 = Ray.call(Adder::add, secondAdder, 1);
    Assert.assertEquals(1, (int) result2.get());

    RayObject<Integer> result3 = Ray.call(Adder::add2, 1);
    Assert.assertEquals(2, (int) result3.get());

    RayObject<Integer> result4 = Ray.call(ActorTest::sayWorld, 2, adder);
    Assert.assertEquals(14, (int) result4.get());

    RayActor<Adder2> adder2 = Ray.create(Adder2.class);
    Ray.call(Adder2::setAdder, adder2, adder);
    RayObject<Integer> result5 = Ray.call(Adder2::increase, adder2);
    Assert.assertEquals(1, (int) result5.get());

    List list = new ArrayList<>();
    list.add(adder);
    Ray.call(Adder2::setAdderList, adder2, list);

    RayObject<Integer> result7 = Ray.call(Adder2::testActorList, adder2);
    Assert.assertEquals(14, (int) result7.get());

    List tempList = new ArrayList<>();
    tempList.add(result);
    Ray.call(Adder::setObjectList, adder, tempList);
    RayObject<Integer> result8 = Ray.call(Adder::testObjectList, adder);
    Assert.assertEquals(11, (int) result8.get());
  }

  @RayRemote
  public static class Adder {

    private List<RayObject<Integer>> objectList;

    public Integer set(Integer n) {
      sum = n;
      return sum;
    }

    public Integer increase() {
      return (++sum);
    }

    public Integer add(Integer n) {
      return (sum += n);
    }

    public static Integer add2(Integer n) {
      return n + 1;
    }

    public Integer setObjectList(List<RayObject<Integer>> objectList) {
      this.objectList = objectList;
      return 1;
    }

    public Integer testObjectList() {
      return ((RayObject<Integer>) objectList.get(0)).get();
    }

    private Integer sum = 0;
  }

  @RayRemote
  public static Integer sayWorld(Integer n, RayActor<ActorTest.Adder> adder) {
    RayObject<Integer> result = Ray.call(ActorTest.Adder::add, adder, 1);
    return result.get() + n;
  }

  @RayRemote
  public static class Adder2 {

    private RayActor<Adder> adder;

    private List<RayActor<Adder>> adderList;

    private UniqueID id;

    public Integer set(Integer n) {
      sum = n;
      return sum;
    }

    public Integer increase() {
      RayObject<Integer> result = Ray.call(Adder::increase, adder);
      Assert.assertEquals(13, (int) result.get());
      return (++sum);
    }

    public Integer testActorList() {
      RayActor<Adder> temp = adderList.get(0);
      RayObject<Integer> result = Ray.call(Adder::increase, temp);
      return result.get();
    }

    public Integer add(Integer n) {
      return (sum += n);
    }

    public static Integer add2(Adder a, Integer n) {
      return n + 1;
    }

    public RayActor<Adder> getAdder() {
      return adder;
    }

    public Integer setAdder(RayActor<Adder> adder) {
      this.adder = adder;
      return 0;
    }

    public UniqueID getId() {
      return id;
    }

    public Integer setId(UniqueID id) {
      this.id = id;
      adder = new RayActor<>(id);
      return 0;
    }

    public Integer setAdderList(List<RayActor<Adder>> adderList) {
      this.adderList = adderList;
      return 0;
    }

    private Integer sum = 0;
  }
}
