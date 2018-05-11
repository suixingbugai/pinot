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

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.api.resources.ControllerApplicationException;
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

  public static String constructSegmentLocation(String storageDirectory, SegmentMetadata segmentMetadata) {
    try {
      return storageDirectory + SLASH + segmentMetadata.getTableName() + SLASH + segmentMetadata.getName() + SLASH + UUID.randomUUID();
    } catch (Exception e) {
      LOGGER.info("Could not construct URI for segment {}", segmentMetadata.getName());
      throw new ControllerApplicationException(LOGGER, "Could not construct segment location", Response.Status.NOT_FOUND);
    }
  }
}
