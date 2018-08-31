package org.ray.spi;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface FileStoreLink {

  boolean mkdirs(String f);

  /**
   * Check if exists.
   *
   * @param f source file
   */
  boolean exists(String f);

  /**
   * True if the named path is a directory.
   *
   * @param f path to check
   */
  boolean isDirectory(String f);

  /**
   * True if the named path is a regular file.
   *
   * @param f path to check
   */
  boolean isFile(String f);

  /**
   * delete a file.
   *
   * @param f         the path to delete.
   * @param recursive if path is a directory and set to true, the directory is deleted else throws
   *                  an exception. In case of a file the recursive can be set to either true or
   *                  false.
   * @return true if delete is successful else false.
   */
  boolean delete(String f, boolean recursive);

  /**
   * The src file is on the local disk.  Add it to FS at the given dst name and the source is kept
   * intact afterwards
   *
   * @param src path
   * @param dst path
   */
  void copyFromLocalFile(String src, String dst);

  /**
   * The src file is under FS, and the dst is on the local disk. Copy it from FS control to the
   * local dst name.
   *
   * @param src path
   * @param dst path
   */
  void copyToLocalFile(String src, String dst);

  /**
   * Create an FSDataOutputStream at the indicated Path. Files are overwritten by default.
   *
   * @param f the file to create
   */
  DataOutputStream create(String f, boolean overwrite);

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f the file name to open
   */
  DataInputStream open(String f);

  /**
   * Append to an existing file (optional operation).
   *
   * @param f the existing file to be appended.
   */
  DataOutputStream append(String f);

  /**
   * get the file length which is located in the file store.
   *
   * @param f the existing file path.
   */
  int fileLength(String f);

}
