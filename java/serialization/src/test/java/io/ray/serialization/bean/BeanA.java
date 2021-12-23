package io.ray.serialization.bean;

import io.ray.serialization.util.StringUtils;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import lombok.Data;

@Data
public class BeanA implements Serializable {
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
    beanA.setF17(StringUtils.random(40));

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
