package com.linkedin.pinot.controller.api.storage;

import org.apache.commons.configuration.Configuration;


/**
 * Factory to initialize the LocalPinotFS filesystem implementation.
 */
public class LocalPinotFSFactory extends PinotFSFactory {
  private final PinotFS _pinotFS;
  public LocalPinotFSFactory() {
    _pinotFS = new LocalPinotFS();
  }

  public void init(Configuration configuration) {
    _pinotFS.init(configuration);
  }

  public PinotFS create() {
    return _pinotFS;
  }
}
