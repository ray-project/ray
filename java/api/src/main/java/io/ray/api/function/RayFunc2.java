// generated automatically, do not modify.

package io.ray.api.function;

/**
 * Functional interface for a remote function that has 2 parameters.
 */
@FunctionalInterface
public interface RayFunc2<T0, T1, R> extends RayFuncR<R> {

  R apply(T0 t0, T1 t1) throws Exception;
}
