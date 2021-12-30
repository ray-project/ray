package io.ray.runtime.serialization;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import lombok.Data;

public class ComplexObjects {
  @Data
  public static class BeanA implements Serializable {
    private short f1;
    private Integer f2;
    private long f3;
    private Float f4;
    private double f5;
    private BeanB beanB;
    private int[] intArray;
    private byte[] bytes;
    private boolean f12;
    private transient BeanB f13;
    public Integer f15;
    public BigDecimal f16;
    public String f17;
    private List<Double> doubleList;
    private Iterable<BeanB> beanBIterable;
    private List<BeanB> beanBList;
    private Map<String, BeanB> stringBeanBMap;
    private int[][] int2DArray;
    private List<List<Double>> double2DList;

    public static BeanA createBeanA(int arrSize) {
      BeanA beanA = new BeanA();
      Random rnd = new Random(37);
      beanA.setF1((short) rnd.nextInt());
      beanA.setF2(rnd.nextInt());
      beanA.setF3(rnd.nextLong());
      beanA.setF4(rnd.nextFloat());
      beanA.setF5(rnd.nextDouble());
      beanA.f15 = rnd.nextInt();
      beanA.setF12(true);
      beanA.setBeanB(BeanB.createBeanB(arrSize));
      BigDecimal decimal = new BigDecimal(new BigInteger("122222222222222225454657712222222222"));
      beanA.setF16(decimal);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 100; i++) {
        sb.append("s");
      }
      beanA.setF17(sb.toString());
      if (arrSize > 0) {
        {
          beanA.bytes = new byte[arrSize];
          rnd.nextBytes(beanA.bytes);
        }
        {
          List<Double> doubleList = new ArrayList<>();
          for (int i = 0; i < arrSize; i++) {
            doubleList.add(rnd.nextDouble());
          }
          doubleList.set(0, null);
          beanA.setDoubleList(doubleList);
        }
        {
          List<List<Double>> double2DList = new ArrayList<>();
          for (int i = 0; i < arrSize; i++) {
            List<Double> dList = new ArrayList<>();
            for (int j = 0; j < arrSize; j++) {
              dList.add(rnd.nextDouble());
            }
            double2DList.add(dList);
          }
          beanA.setDouble2DList(double2DList);
        }
        {
          int[] arr = new int[arrSize];
          for (int i = 0; i < arr.length; i++) {
            arr[i] = rnd.nextInt();
          }
          beanA.setIntArray(arr);
        }
        {
          int[][] int2DArray = new int[arrSize][arrSize];
          for (int i = 0; i < int2DArray.length; i++) {
            int[] arr = int2DArray[i];
            for (int j = 0; j < arr.length; j++) {
              arr[i] = rnd.nextInt();
            }
          }
          beanA.setInt2DArray(int2DArray);
        }
        {
          List<BeanB> beanBList = new ArrayList<>();
          for (int i = 0; i < arrSize; i++) {
            beanBList.add(BeanB.createBeanB(arrSize));
          }
          beanA.setBeanBList(beanBList);
        }
        {
          Map<String, BeanB> stringBeanBMap = new HashMap<>();
          for (int i = 0; i < arrSize; i++) {
            stringBeanBMap.put("key" + i, BeanB.createBeanB(arrSize));
          }
          beanA.setStringBeanBMap(stringBeanBMap);
        }
        {
          List<BeanB> beanBList = new ArrayList<>();
          for (int i = 0; i < arrSize; i++) {
            beanBList.add(BeanB.createBeanB(arrSize));
          }
          beanA.setBeanBIterable(beanBList);
        }
      }
      return beanA;
    }
  }

  @Data
  public static final class BeanB implements Serializable {
    private short f1;
    private Integer f2;
    private long f3;
    private Float f4;
    private double f5;
    private int[] intArr;
    private List<Integer> intList;

    public static BeanB createBeanB(int arrSize) {
      Random rnd = new Random(37);
      BeanB beanB = new BeanB();
      beanB.setF1((short) rnd.nextInt());
      beanB.setF2(rnd.nextInt());
      beanB.setF3(rnd.nextLong());
      beanB.setF4(rnd.nextFloat());
      beanB.setF5(rnd.nextDouble());

      if (arrSize > 0) {
        {
          int[] arr = new int[arrSize];
          for (int i = 0; i < arr.length; i++) {
            arr[i] = rnd.nextInt();
          }
          beanB.setIntArr(arr);
        }
        {
          List<Integer> integers = new ArrayList<>();
          for (int i = 0; i < arrSize; i++) {
            integers.add(rnd.nextInt());
          }
          beanB.setIntList(integers);
        }
      }
      return beanB;
    }
  }

  public static final class Cyclic implements Serializable {
    public Cyclic cyclic;
    public String f1;

    @SuppressWarnings("EqualsWrongThing")
    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      Cyclic cyclic1 = (Cyclic) object;
      if (cyclic != this) {
        return Objects.equals(cyclic, cyclic1.cyclic) && Objects.equals(f1, cyclic1.f1);
      } else {
        return cyclic1.cyclic == cyclic1 && Objects.equals(f1, cyclic1.f1);
      }
    }

    @Override
    public int hashCode() {
      if (cyclic != this) {
        return Objects.hash(cyclic, f1);
      } else {
        return f1.hashCode();
      }
    }

    public static Cyclic create(boolean circular) {
      Cyclic cyclic = new Cyclic();
      cyclic.f1 = "str";
      if (circular) {
        cyclic.cyclic = cyclic;
      }
      return cyclic;
    }
  }

  @Data
  public static class Foo implements Serializable {
    int f1, f2, f3, f4, f5;
    long f6, f7, f8, f9, f10;
    float f11, f12, f13, f14, f15;
    double f16, f17, f18, f19, f20;

    public static Foo create() {
      Random random = new Random();
      Foo foo = new Foo();
      foo.f1 = random.nextInt();
      foo.f3 = random.nextInt();
      foo.f7 = random.nextLong();
      foo.f10 = random.nextLong();
      foo.f13 = random.nextFloat();
      foo.f15 = random.nextFloat();
      foo.f18 = random.nextDouble();
      foo.f20 = random.nextDouble();
      return foo;
    }
  }
}
