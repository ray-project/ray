package io.ray.serialization;

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
import io.ray.serialization.bean.BeanA;
import io.ray.serialization.serializers.BufferCallback;
import io.ray.serialization.serializers.DefaultSerializer;
import io.ray.serialization.serializers.SerializedObject;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.Platform;
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
public class FuryTest {

  @DataProvider(name = "referenceTrackingConfig")
  public static Object[][] referenceTrackingConfig() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider(name = "oobConfig")
  public static Object[][] oobConfig() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider(name = "javaFury")
  public static Object[][] javaFuryConfig() {
    return new Object[][] {
      {Fury.builder().withReferenceTracking(true).withCodegen(true).build()},
      {Fury.builder().withReferenceTracking(false).withCodegen(true).build()},
      {Fury.builder().withReferenceTracking(true).withCodegen(false).build()},
      {Fury.builder().withReferenceTracking(false).withCodegen(false).build()},
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

  @Test(dataProvider = "javaFury")
  public void basicTest(Fury fury) {
    assertEquals(true, serDe(fury, true));
    assertEquals((byte) 1, serDe(fury, (byte) 1));
    assertEquals('a', serDe(fury, 'a'));
    assertEquals((short) 1, serDe(fury, (short) 1));
    assertEquals(1, serDe(fury, 1));
    assertEquals(1L, serDe(fury, 1L));
    assertEquals(Byte.MAX_VALUE, serDe(fury, Byte.MAX_VALUE));
    assertEquals(Short.MAX_VALUE, serDe(fury, Short.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, serDe(fury, Integer.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, serDe(fury, Long.MAX_VALUE));
    assertEquals(Float.MAX_VALUE, serDe(fury, Float.MAX_VALUE));
    assertEquals(Double.MAX_VALUE, serDe(fury, Double.MAX_VALUE));
    assertEquals("str", serDe(fury, "str"));
    assertEquals("str", serDe(fury, new StringBuilder("str")).toString());
    assertEquals("str", serDe(fury, new StringBuffer("str")).toString());
    assertEquals(EnumFoo.A, serDe(fury, EnumFoo.A));
    assertEquals(EnumFoo.B, serDe(fury, EnumFoo.B));
    assertEquals(EnumSubClass.A, serDe(fury, EnumSubClass.A));
    assertEquals(EnumSubClass.B, serDe(fury, EnumSubClass.B));
    assertEquals(BigInteger.valueOf(100), serDe(fury, BigInteger.valueOf(100)));
    assertEquals(BigDecimal.valueOf(100, 2), serDe(fury, BigDecimal.valueOf(100, 2)));
    java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
    assertEquals(sqlDate, serDe(fury, sqlDate));
    LocalDate localDate = LocalDate.now();
    assertEquals(localDate, serDe(fury, localDate));
    Date utilDate = new Date();
    assertEquals(utilDate, serDe(fury, utilDate));
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    assertEquals(timestamp, serDe(fury, timestamp));
    Instant instant = Instant.now();
    assertEquals(instant, serDe(fury, instant));

    assertTrue(
      Arrays.equals(
        new boolean[] {false, true}, (boolean[]) serDe(fury, new boolean[] {false, true})));
    assertEquals(new byte[] {1, 1}, (byte[]) serDe(fury, new byte[] {1, 1}));
    assertEquals(new short[] {1, 1}, (short[]) serDe(fury, new short[] {1, 1}));
    assertEquals(new int[] {1, 1}, (int[]) serDe(fury, new int[] {1, 1}));
    assertEquals(new long[] {1, 1}, (long[]) serDe(fury, new long[] {1, 1}));
    assertTrue(Arrays.equals(new float[] {1.f, 1.f}, (float[]) serDe(fury, new float[] {1f, 1f})));
    assertTrue(
      Arrays.equals(new double[] {1.0, 1.0}, (double[]) serDe(fury, new double[] {1.0, 1.0})));
    assertEquals(new String[] {"str", "str"}, (Object[]) serDe(fury, new String[] {"str", "str"}));
    assertEquals(new Object[] {"str", 1}, (Object[]) serDe(fury, new Object[] {"str", 1}));
    assertTrue(
      Arrays.deepEquals(
        new Integer[][] {{1, 2}, {1, 2}},
        (Integer[][]) serDe(fury, new Integer[][] {{1, 2}, {1, 2}})));

    assertEquals(Arrays.asList(1, 2), serDe(fury, Arrays.asList(1, 2)));
    List<String> arrayList = Arrays.asList("str", "str");
    assertEquals(arrayList, serDe(fury, arrayList));
    assertEquals(new LinkedList<>(arrayList), serDe(fury, new LinkedList<>(arrayList)));
    assertEquals(new HashSet<>(arrayList), serDe(fury, new HashSet<>(arrayList)));
    TreeSet<String> treeSet = new TreeSet<>(Comparator.naturalOrder());
    treeSet.add("str1");
    treeSet.add("str2");
    assertEquals(treeSet, serDe(fury, treeSet));

    HashMap<String, Integer> hashMap = new HashMap<>();
    hashMap.put("k1", 1);
    hashMap.put("k2", 2);
    assertEquals(hashMap, serDe(fury, hashMap));
    assertEquals(new LinkedHashMap<>(hashMap), serDe(fury, new LinkedHashMap<>(hashMap)));
    TreeMap<String, Integer> treeMap = new TreeMap<>(Comparator.naturalOrder());
    treeMap.putAll(hashMap);
    assertEquals(treeMap, serDe(fury, treeMap));
    assertEquals(Collections.EMPTY_LIST, serDe(fury, Collections.EMPTY_LIST));
    assertEquals(Collections.EMPTY_SET, serDe(fury, Collections.EMPTY_SET));
    assertEquals(Collections.EMPTY_MAP, serDe(fury, Collections.EMPTY_MAP));
    assertEquals(Collections.singletonList("str"), serDe(fury, Collections.singletonList("str")));
    assertEquals(Collections.singleton("str"), serDe(fury, Collections.singleton("str")));
    assertEquals(Collections.singletonMap("k", 1), serDe(fury, Collections.singletonMap("k", 1)));
  }

  private Object serDe(Fury fury, Object obj) {
    byte[] bytes = fury.serialize(obj);
    return fury.deserialize(bytes);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void serializeBeanTest(boolean referenceTracking) {
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    BeanA beanA = BeanA.createBeanA(2);
    byte[] bytes = fury.serialize(beanA);
    Object o = fury.deserialize(bytes);
    assertEquals(beanA, o);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void registerTest(boolean referenceTracking) {
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    fury.register(BeanA.class);
    BeanA beanA = BeanA.createBeanA(2);
    assertEquals(beanA, serDe(fury, beanA));
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
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
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
    assertEquals(set, serDe(fury, set));
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
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
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
    assertEquals(map, serDe(fury, map));
    BeanForMap beanForMap = new BeanForMap();
    assertEquals(beanForMap, serDe(fury, beanForMap));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testOffHeap(boolean referenceTracking) {
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    long ptr = 0;
    try {
      int size = 1024;
      ptr = Platform.allocateMemory(size);
      MemoryBuffer buffer = fury.serialize(new A(), ptr, size);
      assertNull(buffer.getHeapMemory());

      Object obj = fury.deserialize(ptr, size);
      assertEquals(new A(), obj);
    } finally {
      Platform.freeMemory(ptr);
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testLambda(boolean referenceTracking) {
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    BiFunction<Fury, Object, byte[]> function =
      (Serializable & BiFunction<Fury, Object, byte[]>) (fury1, obj) -> fury1.serialize(obj);
    fury.deserialize(fury.serialize(function));
  }

  @SuppressWarnings("unchecked")
  @Test(dataProvider = "referenceTrackingConfig")
  public void testJdkProxy(boolean referenceTracking) {
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    Function function =
      (Function)
        Proxy.newProxyInstance(
          fury.getClassLoader(),
          new Class[] {Function.class},
          (Serializable & InvocationHandler) (proxy, method, args) -> 1);
    Function deserializedFunction = (Function) fury.deserialize(fury.serialize(function));
    assertEquals(deserializedFunction.apply(null), 1);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSerializeClasses(boolean referenceTracking) {
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    Primitives.allPrimitiveTypes()
      .forEach(
        cls -> {
          assertSame(cls, fury.deserialize(fury.serialize(cls)));
        });
    Primitives.allWrapperTypes()
      .forEach(
        cls -> {
          assertSame(cls, fury.deserialize(fury.serialize(cls)));
        });
    assertSame(Class.class, fury.deserialize(fury.serialize(Class.class)));
    assertSame(Fury.class, fury.deserialize(fury.serialize(Fury.class)));
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
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    Outer outer = new Outer();
    outer.inner = new Outer.Inner();
    fury.deserialize(fury.serialize(outer));
    assertTrue(
      fury.getClassResolver().getSerializer(Outer.Inner.class) instanceof DefaultSerializer);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSerializeJavaConcurrent(boolean referenceTracking) {
    Fury fury = Fury.builder().withReferenceTracking(referenceTracking).build();
    {
      ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      assertEquals(
        new ArrayList<>((Collection<Integer>) serDe(fury, queue)), new ArrayList<>(queue));
    }
    {
      LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      assertEquals(
        new ArrayList<>((Collection<Integer>) serDe(fury, queue)), new ArrayList<>(queue));
    }
    {
      ConcurrentMap<String, Integer> map = Maps.newConcurrentMap();
      map.put("k1", 1);
      map.put("k2", 3);
      assertEquals(serDe(fury, map), map);
    }
  }

  static class B {
    int f1;
  }

  static class C extends B {
    int f1;
  }

  @Test(dataProvider = "javaFury")
  public void testDuplicateFields(Fury fury) {
    C c = new C();
    ((B) c).f1 = 100;
    c.f1 = -100;
    assertEquals(((B) c).f1, 100);
    assertEquals(c.f1, -100);
    C newC = (C) serDe(fury, c);
    assertEquals(newC.f1, c.f1);
    assertEquals(((B) newC).f1, ((B) c).f1);
  }

  @Test
  public void testUnmodifiableCollection() {
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    Assert.assertEquals(
      fury.deserialize(fury.serialize(Collections.unmodifiableList((Lists.newArrayList(1))))),
      Collections.unmodifiableList(Lists.newArrayList(1)));
    Assert.assertEquals(
      fury.deserialize(fury.serialize(Collections.unmodifiableList(Lists.newLinkedList()))),
      Collections.unmodifiableList(Lists.newLinkedList()));
    Assert.assertEquals(
      fury.deserialize(fury.serialize(Collections.unmodifiableSet(Sets.newHashSet(1)))),
      Collections.unmodifiableSet(Sets.newHashSet(1)));
    Assert.assertEquals(
      fury.deserialize(fury.serialize(Collections.unmodifiableSortedSet(Sets.newTreeSet()))),
      Collections.unmodifiableSortedSet(Sets.newTreeSet()));
    Assert.assertEquals(
      fury.deserialize(fury.serialize(Collections.unmodifiableMap(ImmutableMap.of(1, 2)))),
      Collections.unmodifiableMap(ImmutableMap.of(1, 2)));
    Assert.assertEquals(
      fury.deserialize(fury.serialize(Collections.unmodifiableSortedMap(new TreeMap<>()))),
      Collections.unmodifiableSortedMap(new TreeMap<>()));
  }

  @Test
  public void testJDKSerializable() throws Exception {
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    serDe(fury, ByteBuffer.allocate(32));
    serDe(fury, ByteBuffer.allocateDirect(32));
    assertThrows(
      UnsupportedOperationException.class,
      () -> fury.serialize(new Thread()));
  }

  @Test(dataProvider = "oobConfig")
  public void testOutOfBandSerialization(boolean oob) {
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    List<Object> list = Arrays.asList(new byte[10000], ByteBuffer.allocate(10000));
    Map<Object, Object> map = new HashMap<>();
    map.put("k1", list);
    map.put("k2", ByteBuffer.allocateDirect(10000));
    Collection<SerializedObject> serializedObjects = new ArrayList<>();
    BufferCallback bufferCallback = o -> !serializedObjects.add(o);
    if (!oob) {
      bufferCallback = null;
    }
    byte[] inBandBuffer = fury.serialize(map, bufferCallback);
    if (oob) {
      assertEquals(serializedObjects.size(), 3);
    }
    List<ByteBuffer> buffers = serializedObjects.stream()
      .map(SerializedObject::toBuffer).collect(Collectors.toList());
    if (!oob) {
      buffers = null;
    }
    Map<Object, Object> newMap = (Map<Object, Object>) fury.deserialize(inBandBuffer, buffers);
    List<Object> newList = (List<Object>) newMap.get("k1");
    assertEquals(newList.size(), 2);
    assertEquals(((byte[]) newList.get(0)).length, 10000);
    assertEquals(((ByteBuffer) newList.get(1)).remaining(), 10000);
  }
}
