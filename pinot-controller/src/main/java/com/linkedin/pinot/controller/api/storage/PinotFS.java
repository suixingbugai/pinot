/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.storage;

import org.apache.commons.configuration.Configuration;

/**
 * The PinotFS is intended to be a thin wrapper on top of different filesystems. Each storage type that is implemented
 * will implement this class for filesystem-specific operations
 */
public interface PinotFS {
  /**
   * Initializes the configurations specific to that filesystem. For instance, any security related parameters can be
   * initialized here
   * @param config
   */
  void init(Configuration config);

  /**
   * Deletes the file at the location provided
   * @param location
   * @return
   * @throws Exception
   */
  boolean delete(String location) throws Exception;

  /**
   * Moves the file from the src to dst. Does not keep the original file. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories.
   * @param src
   * @param dst
   * @return
   * @throws Exception
   */
  boolean move(String src, String dst) throws Exception;

  /**
   * Copies a file from src to dst. Keeps the original file. If the dst has parent directories that haven't
   * been created, this method will create all the necessary parent directories.
   * @param src
   * @param dst
   * @return
   * @throws Exception
   */
  boolean copy(String src, String dst) throws Exception;

  /**
   * Checks whether the file at the provided location exists.
   * @param location
   * @return
   */
  boolean exists(String location);

  /**
   * Returns the length of the file at the provided location.
   * @param location
   * @return
   */
  long length(String location);

  /**
   * Lists all the files at the location provided. Returns null if this abstract pathname does not denote a directory, or if
   * an I/O error occurs.
   * @param location
   * @return
   */
  String[] listFiles(String location);

  /**
   * Returns the abstract pathname of this abstract pathname's parent, or null if this pathname does not name a parent
   * directory. The parent consists of the pathname's prefix, if any, and each name in the pathname's name sequence
   * except for the last. If the name sequence is empty, then the pathname does not name a parent directory.
   * @param location
   * @return
   */
  String getParentFile(String location);

  /**
   * Returns the name of the file or directory denoted by this abstract pathname. This is just the last name in the
   * pathname's name sequence. If the pathname's name sequence is empty, then the empty string is returned.
   * @param location
   * @return
   */
  String getName(String location);
}
