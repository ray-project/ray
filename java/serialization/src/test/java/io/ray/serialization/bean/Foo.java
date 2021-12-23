package io.ray.serialization.bean;

import java.io.Serializable;
import java.util.Random;
import lombok.Data;

@Data
public class Foo implements Serializable {
  int f1;
  int f2;
  long f3;
  long f4;
  float f5;
  double f6;
  double f7;
  long f8;
  long f9;
  long f10;
  long f11;
  long f12;
  long f13;
  long f14;
  long f15;

  public static Foo create() {
    Random random = new Random();
    Foo foo = new Foo();
    foo.f1 = random.nextInt();
    foo.f2 = random.nextInt();
    foo.f3 = random.nextLong();
    foo.f4 = random.nextLong();
    foo.f5 = random.nextFloat();
    foo.f6 = random.nextDouble();
    foo.f7 = random.nextDouble();
    foo.f8 = random.nextLong();
    foo.f9 = random.nextLong();
    foo.f10 = random.nextLong();
    foo.f11 = random.nextLong();
    foo.f13 = random.nextLong();
    foo.f14 = random.nextLong();
    foo.f15 = random.nextLong();
    return foo;
  }
}
