package org.ray.spi.impl;

/**
 * This exception is raised if the object could not be created because the plasma store is unable to
 * evict enough objects to create room for it.
 */
public class PlasmaOutOfMemoryException extends Exception {

  /**
   *
   */
  private static final long serialVersionUID = -2786069077559520659L;

  public PlasmaOutOfMemoryException() {
    super();
  }

  public PlasmaOutOfMemoryException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public PlasmaOutOfMemoryException(String message, Throwable cause) {
    super(message, cause);
  }

  public PlasmaOutOfMemoryException(String message) {
    super(message);
  }

  public PlasmaOutOfMemoryException(Throwable cause) {
    super(cause);
  }

}
