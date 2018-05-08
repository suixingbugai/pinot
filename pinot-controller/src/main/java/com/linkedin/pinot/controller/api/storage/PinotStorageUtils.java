package com.linkedin.pinot.controller.api.storage;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.api.resources.ControllerApplicationException;
import java.net.URI;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods for storage operations.
 */
public class PinotStorageUtils {
  private static final String SLASH = "/";
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotStorageUtils.class);

  public static URI constructSegmentLocation(String storageDirectory, SegmentMetadata segmentMetadata) {
    try {
      return new URI(storageDirectory + SLASH + segmentMetadata.getTableName() + SLASH + segmentMetadata.getName() + SLASH + UUID.randomUUID());
    } catch (Exception e) {
      LOGGER.info("Could not construct URI for segment {}", segmentMetadata.getName());
      throw new ControllerApplicationException(LOGGER, "Could not construct segment location", Response.Status.NOT_FOUND);
    }
  }
}
