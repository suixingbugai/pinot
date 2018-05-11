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
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;


/**
 * Local implementation of the Pinot filesystem
 */
public class LocalPinotFS implements PinotFS {

  public LocalPinotFS() {
  }

  public void init(Configuration configuration) {
  }

  public boolean delete(String location) throws Exception {
    File file = new File(location);
    if (file.isDirectory()) {
      FileUtils.deleteDirectory(file);
      return true;
    }
    return file.delete();
  }

  public boolean move(String src, String dst) throws Exception {
    File srcFile = new File(src);
    File dstFile = new File(dst);
    dstFile.getParentFile().mkdirs();
    if (srcFile.renameTo(dstFile) && srcFile.delete()) {
      return true;
    }
    return false;
  }

  public boolean copy(String src, String dst) throws Exception {
    File srcFile = new File(src);
    File dstFile = new File(dst);
    dstFile.getParentFile().mkdirs();
    if (srcFile.renameTo(dstFile)) {
      return true;
    }
    return false;
  }

  public boolean exists(String location) {
    File file = new File(location);
    return file.exists();
  }

  public long length(String location) {
    File file = new File(location);
    return file.length();
  }

  public String[] listFiles(String location) {
    File file = new File(location);
    return file.list();
  }

  public String getParentFile(String location) {
    File file = new File(location);
    return file.getParentFile().getPath();
  }

  public String getName(String location) {
    File file = new File(location);
    return file.getName();
  }
}
