package io.ray.serialization.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.Data;

@Data
public final class BeanB implements Serializable {
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
