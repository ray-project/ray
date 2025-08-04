package io.ray.serve.poll;

/** Listener of long poll. It notifies changed object to the specified key. */
@FunctionalInterface
public interface KeyListener {

  void notifyChanged(Object updatedObject);
}
