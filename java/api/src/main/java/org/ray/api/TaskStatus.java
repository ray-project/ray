package org.ray.api;

import java.io.Serializable;

/**
 * Represents a status of type T of a task.
 */
public class TaskStatus<T> implements Serializable {

  private static final long serialVersionUID = -3382082416577683751L;

  public T status;
}
