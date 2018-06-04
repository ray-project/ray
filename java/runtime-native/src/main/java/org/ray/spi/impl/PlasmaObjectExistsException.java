package org.ray.spi.impl;

/**
 * This exception is raised if the object could not be created because there already is an object
 * with the same ID in the plasma store.
 */
public class PlasmaObjectExistsException extends Exception {

  /**
   *
   */
  private static final long serialVersionUID = 9128880292504270291L;

  public PlasmaObjectExistsException() {
    super();
  }

  public PlasmaObjectExistsException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public PlasmaObjectExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public PlasmaObjectExistsException(String message) {
    super(message);
  }

  public PlasmaObjectExistsException(Throwable cause) {
    super(cause);
  }

}
