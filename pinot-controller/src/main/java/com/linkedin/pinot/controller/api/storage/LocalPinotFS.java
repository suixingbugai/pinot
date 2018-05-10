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

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.configuration.Configuration;


/**
 * Local implementation of the Pinot filesystem
 */
public class LocalPinotFS implements PinotFS {
  public static String _storageDir;
  public static final String PINOT_FS_STORAGE_LOCATION = "dir";

  public LocalPinotFS() {
  }

  public void init(Configuration configuration) {
    _storageDir = configuration.getString(PINOT_FS_STORAGE_LOCATION);
  }

  public boolean delete(URI uri) throws Exception {
    File file = new File(_storageDir, uri.getPath());
    return file.delete();
  }

  public boolean move(URI srcUri, URI dstUri) throws Exception {
    Files.move(Paths.get(_storageDir, srcUri.toString()), Paths.get(_storageDir, dstUri.toString()));
    return true;
  }

  public boolean copy(URI srcUri, URI dstUri) throws Exception {
    Files.copy(Paths.get(_storageDir, srcUri.toString()), Paths.get(_storageDir, dstUri.toString()));
    return true;
  }

  public boolean exists(URI uri) {
    File file = new File(_storageDir, uri.getPath());
    return file.exists();
  }

  public long length(URI uri) {
    File file = new File(_storageDir, uri.getPath());
    return file.length();
  }

  public String[] listFiles(URI uri) {
    File file = new File(_storageDir, uri.getPath());
    return file.list();
  }

  public String getParentFile(URI uri) {
    File file = new File(_storageDir, uri.getPath());
    return file.getParentFile().getPath();
  }

  public String getName(URI uri) {
    File file = new File(_storageDir, uri.getPath());
    return file.getName();
  }
}
