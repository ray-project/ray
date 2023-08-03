package io.ray.api.concurrencygroup;

import io.ray.api.Ray;
import io.ray.api.function.RayFunc;
import java.util.ArrayList;
import java.util.List;

public class ConcurrencyGroupBuilder<A> extends BaseConcurrencyGroupBuilder<A> {

  private String name;

  private int maxConcurrency;

  private List<RayFunc> funcs = new ArrayList<>();

  public ConcurrencyGroupBuilder<A> setName(String name) {
    this.name = name;
    return this;
  }

  public ConcurrencyGroupBuilder<A> setMaxConcurrency(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
    return this;
  }

  public ConcurrencyGroup build() {
    return Ray.internal().createConcurrencyGroup(this.name, this.maxConcurrency, funcs);
  }

  @Override
  protected ConcurrencyGroupBuilder<A> internalAddMethod(RayFunc func) {
    funcs.add(func);
    return this;
  }
}
