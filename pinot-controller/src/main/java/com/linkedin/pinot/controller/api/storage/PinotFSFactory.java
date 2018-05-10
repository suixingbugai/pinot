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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Initializes the PinotFS implementation.
 */
public abstract class PinotFSFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotFSFactory.class);
  public static final String PINOT_FS_CLASS_CONFIG = "class";

  public abstract void init(Configuration configuration);

  public abstract PinotFS create();

  public static PinotFSFactory loadFactory(Configuration configuration) {
    PinotFSFactory pinotFSFactory;
    String pinotFSFactoryClassName = configuration.getString(PINOT_FS_CLASS_CONFIG);
    if (pinotFSFactoryClassName == null) {
      pinotFSFactoryClassName = LocalPinotFSFactory.class.getName();
    }
    try {
      LOGGER.info("Instantiating Pinot FS factory class {}", pinotFSFactoryClassName);
      pinotFSFactory =  (PinotFSFactory) Class.forName(pinotFSFactoryClassName).newInstance();
      LOGGER.info("Initializing Pinot FS factory class {}", pinotFSFactoryClassName);
      pinotFSFactory.init(configuration);
      return pinotFSFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
