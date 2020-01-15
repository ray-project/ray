// generated automatically, do not modify.

package org.ray.api.function;

/**
 * Functional interface for a remote function that has 3 parameters.
 */
@FunctionalInterface
public interface RayFunc3<T0, T1, T2, R> extends RayFunc {

  R apply(T0 t0, T1 t1, T2 t2) throws Exception;
}
