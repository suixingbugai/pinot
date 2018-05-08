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
