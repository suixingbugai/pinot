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
package com.linkedin.pinot.controller.storage;

import com.linkedin.pinot.controller.api.storage.LocalPinotFS;
import java.io.File;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class LocalPinotFSTest {
  LocalPinotFS _localPinotFS = new LocalPinotFS();
  private static final File JAVA_TMPDIR = FileUtils.getTempDirectory();
  private static final File TEMP_DIR = new File(JAVA_TMPDIR, "LocalPinotFSTest");
  private static final File TEMP_DIR_COPY = new File(JAVA_TMPDIR, "LocalPinotFSTestTmp");

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    FileUtils.deleteDirectory(TEMP_DIR_COPY);
    TEMP_DIR.mkdir();
    File file = new File(TEMP_DIR, "segmentName");
    file.createNewFile();
  }

  @Test
  public void testFSOperations() throws Exception {
    boolean copied = _localPinotFS.copy(TEMP_DIR.getPath(), TEMP_DIR_COPY.getPath());
    Assert.assertTrue(copied);
    boolean deleted = _localPinotFS.delete(TEMP_DIR_COPY.getPath());
    Assert.assertTrue(deleted);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    FileUtils.deleteDirectory(TEMP_DIR_COPY);
  }}
