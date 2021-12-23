package io.ray.serialization.bean;

import java.util.Objects;

public final class Cyclic {
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
