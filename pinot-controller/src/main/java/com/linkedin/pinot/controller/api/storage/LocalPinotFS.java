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
