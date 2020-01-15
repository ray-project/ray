// generated automatically, do not modify.

package org.ray.api.function;

/**
 * Functional interface for a remote function that has 2 parameters.
 */
@FunctionalInterface
public interface RayFunc2<T0, T1, R> extends RayFunc {

  R apply(T0 t0, T1 t1) throws Exception;
}
