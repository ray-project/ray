package io.ray.runtime.serialization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Primitives;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.serialization.serializers.BufferCallback;
import io.ray.runtime.serialization.serializers.DefaultSerializer;
import io.ray.runtime.serialization.serializers.SerializedObject;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class RaySerDeTest {

  @DataProvider(name = "referenceTrackingConfig")
  public static Object[][] referenceTrackingConfig() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider(name = "oobConfig")
  public static Object[][] oobConfig() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider(name = "javaSerde")
  public static Object[][] javaSerdeConfig() {
    return new Object[][] {
      {RaySerde.builder().withReferenceTracking(true).build()},
      {RaySerde.builder().withReferenceTracking(false).build()},
      {RaySerde.builder().withReferenceTracking(true).build()},
      {RaySerde.builder().withReferenceTracking(false).build()},
    };
  }

  enum EnumFoo {
    A,
    B
  }

  enum EnumSubClass {
    A {
      @Override
      void f() {
      }
    },
    B {
      @Override
      void f() {
      }
    };

    abstract void f();
  }

  @Test(dataProvider = "javaSerde")
  public void basicTest(RaySerde raySerDe) {
    assertEquals(true, serDe(raySerDe, true));
    assertEquals((byte) 1, serDe(raySerDe, (byte) 1));
    assertEquals('a', serDe(raySerDe, 'a'));
    assertEquals((short) 1, serDe(raySerDe, (short) 1));
    assertEquals(1, serDe(raySerDe, 1));
    assertEquals(1L, serDe(raySerDe, 1L));
    assertEquals(Byte.MAX_VALUE, serDe(raySerDe, Byte.MAX_VALUE));
    assertEquals(Short.MAX_VALUE, serDe(raySerDe, Short.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, serDe(raySerDe, Integer.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, serDe(raySerDe, Long.MAX_VALUE));
    assertEquals(Float.MAX_VALUE, serDe(raySerDe, Float.MAX_VALUE));
    assertEquals(Double.MAX_VALUE, serDe(raySerDe, Double.MAX_VALUE));
    assertEquals("str", serDe(raySerDe, "str"));
    assertEquals("str", serDe(raySerDe, new StringBuilder("str")).toString());
    assertEquals("str", serDe(raySerDe, new StringBuffer("str")).toString());
    assertEquals(EnumFoo.A, serDe(raySerDe, EnumFoo.A));
    assertEquals(EnumFoo.B, serDe(raySerDe, EnumFoo.B));
    assertEquals(EnumSubClass.A, serDe(raySerDe, EnumSubClass.A));
    assertEquals(EnumSubClass.B, serDe(raySerDe, EnumSubClass.B));
    assertEquals(BigInteger.valueOf(100), serDe(raySerDe, BigInteger.valueOf(100)));
    assertEquals(BigDecimal.valueOf(100, 2), serDe(raySerDe, BigDecimal.valueOf(100, 2)));
    java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
    assertEquals(sqlDate, serDe(raySerDe, sqlDate));
    LocalDate localDate = LocalDate.now();
    assertEquals(localDate, serDe(raySerDe, localDate));
    Date utilDate = new Date();
    assertEquals(utilDate, serDe(raySerDe, utilDate));
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    assertEquals(timestamp, serDe(raySerDe, timestamp));
    Instant instant = Instant.now();
    assertEquals(instant, serDe(raySerDe, instant));

    assertTrue(
      Arrays.equals(
        new boolean[] {false, true}, (boolean[]) serDe(raySerDe, new boolean[] {false, true})));
    assertEquals(new byte[] {1, 1}, (byte[]) serDe(raySerDe, new byte[] {1, 1}));
    assertEquals(new short[] {1, 1}, (short[]) serDe(raySerDe, new short[] {1, 1}));
    assertEquals(new int[] {1, 1}, (int[]) serDe(raySerDe, new int[] {1, 1}));
    assertEquals(new long[] {1, 1}, (long[]) serDe(raySerDe, new long[] {1, 1}));
    assertTrue(
      Arrays.equals(new float[] {1.f, 1.f}, (float[]) serDe(raySerDe, new float[] {1f, 1f})));
    assertTrue(
      Arrays.equals(
        new double[] {1.0, 1.0}, (double[]) serDe(raySerDe, new double[] {1.0, 1.0})));
    assertEquals(
      new String[] {"str", "str"}, (Object[]) serDe(raySerDe, new String[] {"str", "str"}));
    assertEquals(new Object[] {"str", 1}, (Object[]) serDe(raySerDe, new Object[] {"str", 1}));
    assertTrue(
      Arrays.deepEquals(
        new Integer[][] {{1, 2}, {1, 2}},
        (Integer[][]) serDe(raySerDe, new Integer[][] {{1, 2}, {1, 2}})));

    assertEquals(Arrays.asList(1, 2), serDe(raySerDe, Arrays.asList(1, 2)));
    List<String> arrayList = Arrays.asList("str", "str");
    assertEquals(arrayList, serDe(raySerDe, arrayList));
    assertEquals(new LinkedList<>(arrayList), serDe(raySerDe, new LinkedList<>(arrayList)));
    assertEquals(new HashSet<>(arrayList), serDe(raySerDe, new HashSet<>(arrayList)));
    TreeSet<String> treeSet = new TreeSet<>(Comparator.naturalOrder());
    treeSet.add("str1");
    treeSet.add("str2");
    assertEquals(treeSet, serDe(raySerDe, treeSet));

    HashMap<String, Integer> hashMap = new HashMap<>();
    hashMap.put("k1", 1);
    hashMap.put("k2", 2);
    assertEquals(hashMap, serDe(raySerDe, hashMap));
    assertEquals(new LinkedHashMap<>(hashMap), serDe(raySerDe, new LinkedHashMap<>(hashMap)));
    TreeMap<String, Integer> treeMap = new TreeMap<>(Comparator.naturalOrder());
    treeMap.putAll(hashMap);
    assertEquals(treeMap, serDe(raySerDe, treeMap));
    assertEquals(Collections.EMPTY_LIST, serDe(raySerDe, Collections.EMPTY_LIST));
    assertEquals(Collections.EMPTY_SET, serDe(raySerDe, Collections.EMPTY_SET));
    assertEquals(Collections.EMPTY_MAP, serDe(raySerDe, Collections.EMPTY_MAP));
    assertEquals(
      Collections.singletonList("str"), serDe(raySerDe, Collections.singletonList("str")));
    assertEquals(Collections.singleton("str"), serDe(raySerDe, Collections.singleton("str")));
    assertEquals(
      Collections.singletonMap("k", 1), serDe(raySerDe, Collections.singletonMap("k", 1)));
  }

  private Object serDe(RaySerde raySerDe, Object obj) {
    byte[] bytes = raySerDe.serialize(obj);
    return raySerDe.deserialize(bytes);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void serializeBeanTest(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    {
      ComplexObjects.BeanB bean = ComplexObjects.BeanB.create(2);
      byte[] bytes = raySerDe.serialize(bean);
      Object o = raySerDe.deserialize(bytes);
      assertEquals(bean, o);
    }
    {
      ComplexObjects.BeanA beanA = ComplexObjects.BeanA.create(2);
      byte[] bytes = raySerDe.serialize(beanA);
      Object o = raySerDe.deserialize(bytes);
      assertEquals(beanA, o);
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void serializeGenericsTest(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    {
      ComplexObjects.MapCollectionBean bean = ComplexObjects.MapCollectionBean.create(2);
      byte[] bytes = raySerDe.serialize(bean);
      Object o = raySerDe.deserialize(bytes);
      assertEquals(bean, o);
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void registerTest(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    raySerDe.register(ComplexObjects.BeanA.class);
    ComplexObjects.BeanA beanA = ComplexObjects.BeanA.create(2);
    assertEquals(beanA, serDe(raySerDe, beanA));
  }

  @EqualsAndHashCode
  static class A implements Serializable {
    public Object f1 = 1;
    public Object f2 = 1;
    private Object f3 = "str";

    Object getF3() {
      return f3;
    }

    void setF3(Object f3) {
      this.f3 = f3;
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testTreeSet(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    TreeSet<String> set =
      new TreeSet<>(
        (Comparator<? super String> & Serializable)
          (s1, s2) -> {
            int delta = s1.length() - s2.length();
            if (delta == 0) {
              return s1.compareTo(s2);
            } else {
              return delta;
            }
          });
    set.add("str11");
    set.add("str2");
    assertEquals(set, serDe(raySerDe, set));
  }

  @Data
  public static class BeanForMap {
    public Map<String, String> map = new TreeMap<>();

    {
      map.put("k1", "v1");
      map.put("k2", "v2");
    }
  }

  @Test
  public void testTreeMap() {
    boolean referenceTracking = true;
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    TreeMap<String, String> map =
      new TreeMap<>(
        (Comparator<? super String> & Serializable)
          (s1, s2) -> {
            int delta = s1.length() - s2.length();
            if (delta == 0) {
              return s1.compareTo(s2);
            } else {
              return delta;
            }
          });
    map.put("str1", "1");
    map.put("str2", "1");
    assertEquals(map, serDe(raySerDe, map));
    BeanForMap beanForMap = new BeanForMap();
    assertEquals(beanForMap, serDe(raySerDe, beanForMap));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testOffHeap(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    long ptr = 0;
    try {
      int size = 1024;
      ptr = Platform.allocateMemory(size);
      MemoryBuffer buffer = raySerDe.serialize(new A(), ptr, size);
      assertNull(buffer.getHeapMemory());

      Object obj = raySerDe.deserialize(ptr, size);
      assertEquals(new A(), obj);
    } finally {
      Platform.freeMemory(ptr);
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testLambda(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    BiFunction<RaySerde, Object, byte[]> function =
      (Serializable & BiFunction<RaySerde, Object, byte[]>)
        (serde1, obj) -> serde1.serialize(obj);
    raySerDe.deserialize(raySerDe.serialize(function));
  }

  @SuppressWarnings("unchecked")
  @Test(dataProvider = "referenceTrackingConfig")
  public void testJdkProxy(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    Function function =
      (Function)
        Proxy.newProxyInstance(
          raySerDe.getClassLoader(),
          new Class[] {Function.class},
          (Serializable & InvocationHandler) (proxy, method, args) -> 1);
    Function deserializedFunction = (Function) raySerDe.deserialize(raySerDe.serialize(function));
    assertEquals(deserializedFunction.apply(null), 1);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSerializeClasses(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    Primitives.allPrimitiveTypes()
      .forEach(
        cls -> {
          assertSame(cls, raySerDe.deserialize(raySerDe.serialize(cls)));
        });
    Primitives.allWrapperTypes()
      .forEach(
        cls -> {
          assertSame(cls, raySerDe.deserialize(raySerDe.serialize(cls)));
        });
    assertSame(Class.class, raySerDe.deserialize(raySerDe.serialize(Class.class)));
    assertSame(RaySerde.class, raySerDe.deserialize(raySerDe.serialize(RaySerde.class)));
  }

  public static class Outer {
    private long x;
    private Inner inner;

    private static class Inner {
      int y;
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSerializePrivateBean(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    Outer outer = new Outer();
    outer.inner = new Outer.Inner();
    raySerDe.deserialize(raySerDe.serialize(outer));
    assertTrue(
      raySerDe.getClassResolver().getSerializer(Outer.Inner.class) instanceof DefaultSerializer);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSerializeJavaConcurrent(boolean referenceTracking) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(referenceTracking).build();
    {
      ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      assertEquals(
        new ArrayList<>((Collection<Integer>) serDe(raySerDe, queue)), new ArrayList<>(queue));
    }
    {
      LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      assertEquals(
        new ArrayList<>((Collection<Integer>) serDe(raySerDe, queue)), new ArrayList<>(queue));
    }
    {
      ConcurrentMap<String, Integer> map = Maps.newConcurrentMap();
      map.put("k1", 1);
      map.put("k2", 3);
      assertEquals(serDe(raySerDe, map), map);
    }
  }

  static class B {
    int f1;
  }

  static class C extends B {
    int f1;
  }

  @Test(dataProvider = "javaSerde")
  public void testDuplicateFields(RaySerde raySerDe) {
    C c = new C();
    ((B) c).f1 = 100;
    c.f1 = -100;
    assertEquals(((B) c).f1, 100);
    assertEquals(c.f1, -100);
    C newC = (C) serDe(raySerDe, c);
    assertEquals(newC.f1, c.f1);
    assertEquals(((B) newC).f1, ((B) c).f1);
  }

  @Test
  public void testUnmodifiableCollection() {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(true).build();
    Assert.assertEquals(
      raySerDe.deserialize(
        raySerDe.serialize(Collections.unmodifiableList((Lists.newArrayList(1))))),
      Collections.unmodifiableList(Lists.newArrayList(1)));
    Assert.assertEquals(
      raySerDe.deserialize(
        raySerDe.serialize(Collections.unmodifiableList(Lists.newLinkedList()))),
      Collections.unmodifiableList(Lists.newLinkedList()));
    Assert.assertEquals(
      raySerDe.deserialize(raySerDe.serialize(Collections.unmodifiableSet(Sets.newHashSet(1)))),
      Collections.unmodifiableSet(Sets.newHashSet(1)));
    Assert.assertEquals(
      raySerDe.deserialize(
        raySerDe.serialize(Collections.unmodifiableSortedSet(Sets.newTreeSet()))),
      Collections.unmodifiableSortedSet(Sets.newTreeSet()));
    Assert.assertEquals(
      raySerDe.deserialize(
        raySerDe.serialize(Collections.unmodifiableMap(ImmutableMap.of(1, 2)))),
      Collections.unmodifiableMap(ImmutableMap.of(1, 2)));
    Assert.assertEquals(
      raySerDe.deserialize(
        raySerDe.serialize(Collections.unmodifiableSortedMap(new TreeMap<>()))),
      Collections.unmodifiableSortedMap(new TreeMap<>()));
  }

  @Test
  public void testJDKSerializable() throws Exception {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(true).build();
    serDe(raySerDe, ByteBuffer.allocate(32));
    serDe(raySerDe, ByteBuffer.allocateDirect(32));
    assertThrows(UnsupportedOperationException.class, () -> raySerDe.serialize(new Thread()));
  }

  @Test(dataProvider = "oobConfig")
  public void testOutOfBandSerialization(boolean oob) {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(true).build();
    List<Object> list = Arrays.asList(new byte[10000], ByteBuffer.allocate(10000));
    Object[] objects = new Object[] {
      new byte[10000], ByteBuffer.allocate(10000), ByteBuffer.allocateDirect(10000)};
    Collection<SerializedObject> serializedObjects = new ArrayList<>();
    BufferCallback bufferCallback = o -> !serializedObjects.add(o);
    if (!oob) {
      bufferCallback = null;
    }
    byte[] inBandBuffer = raySerDe.serialize(objects, bufferCallback);
    if (oob) {
      assertEquals(serializedObjects.size(), 3);
    }
    List<ByteBuffer> buffers =
      serializedObjects.stream().map(SerializedObject::toBuffer).collect(Collectors.toList());
    if (!oob) {
      buffers = null;
    }
    Object[] newObjects = (Object[]) raySerDe.deserialize(inBandBuffer, buffers);
    assertEquals(((byte[]) newObjects[0]).length, 10000);
    assertEquals(((ByteBuffer) newObjects[1]).remaining(), 10000);
    assertEquals(((ByteBuffer) newObjects[2]).remaining(), 10000);
  }
}
