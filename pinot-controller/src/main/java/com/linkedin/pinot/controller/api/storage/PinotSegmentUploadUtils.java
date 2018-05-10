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

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.access.AccessControlFactory;
import com.linkedin.pinot.controller.api.resources.ControllerApplicationException;
import com.linkedin.pinot.controller.api.resources.SuccessResponse;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.net.URI;
import java.util.Date;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.helix.ZNRecord;
import org.joda.time.Interval;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains upload-specific methods
 */
public class PinotSegmentUploadUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadUtils.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  HttpConnectionManager _connectionManager;

  @Inject
  Executor _executor;

  @Inject
  AccessControlFactory _accessControlFactory;

  @Inject
  PinotFSFactory _pinotFSFactory;

  /**
   * Updates zk metadata for uploaded segments
   * @param segmentMetadata
   * @param dstUri final uploaded location of the segment
   * @param segmentUploaderConfig
   */
  public void pushMetadata(SegmentMetadata segmentMetadata, URI dstUri, SegmentUploaderConfig segmentUploaderConfig)
      throws JSONException {
    PinotFS pinotFS = createPinotFS();
    HttpHeaders headers = segmentUploaderConfig.getHeaders();
    String tableName = segmentMetadata.getTableName();
    String segmentName = segmentMetadata.getName();
    String tableNameWithType = getTableNameWithType(segmentName, tableName);

    ZNRecord znRecord = _pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);

    // Segment is stored under its version directory
    String segmentVersionUUID = getSegmentVersionUUID(dstUri, pinotFS);

    // Brand new segment, not refresh, directly add the segment
    if (znRecord == null) {
      _pinotHelixResourceManager.addNewSegment(segmentMetadata, dstUri.toString(), segmentVersionUUID);
      return;
    }

    // Segment already exists, refresh if necessary
    OfflineSegmentZKMetadata existingSegmentZKMetadata = new OfflineSegmentZKMetadata(znRecord);
    long existingCrc = existingSegmentZKMetadata.getCrc();

    // Version of the current segment in zk
    String currentSegmentVersionUUID = existingSegmentZKMetadata.getSegmentVersionUUID();

    // Segment does not have any version, means should not call only metadata method, should have gone through the upload
    if (segmentVersionUUID == null) {
      throw new ControllerApplicationException(LOGGER,
          "Segment location is not expected: " + segmentName + " of table: " + tableName, Response.Status.NOT_ACCEPTABLE);
    }
    if (!segmentVersionUUID.equals(currentSegmentVersionUUID)) {
      // Different segment is in zk, reject push
      throw new ControllerApplicationException(LOGGER,
          "Another segment upload is in progress for segment: " + segmentName + " of table: " + tableName
              + ", retry later", Response.Status.CONFLICT);
    }

    try {
      // Modify the custom map in segment ZK metadata
      String segmentZKMetadataCustomMapModifierStr = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER);
      SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier;
      if (segmentZKMetadataCustomMapModifierStr != null) {
        segmentZKMetadataCustomMapModifier =
            new SegmentZKMetadataCustomMapModifier(segmentZKMetadataCustomMapModifierStr);
      } else {
        // By default, use REPLACE modify mode
        segmentZKMetadataCustomMapModifier =
            new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.REPLACE, null);
      }
      existingSegmentZKMetadata.setCustomMap(
          segmentZKMetadataCustomMapModifier.modifyMap(existingSegmentZKMetadata.getCustomMap()));

      // Update ZK metadata and refresh the segment if necessary
      long newCrc = Long.valueOf(segmentMetadata.getCrc());
      if (newCrc == existingCrc) {
        // New segment is the same as the existing one, only update ZK metadata without refresh the segment
        if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata)) {
          throw new RuntimeException(
              "Failed to update ZK metadata for segment: " + segmentName + " of table: " + tableNameWithType);
        }
      } else {
        _pinotHelixResourceManager.refreshSegment(segmentMetadata, existingSegmentZKMetadata, dstUri.toString(), segmentVersionUUID);
      }
    } catch (Exception e) {
      if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata)) {
        LOGGER.error("Failed to update ZK metadata for segment: {} of table: {}", segmentName, tableNameWithType);
      }
      throw e;
    }
  }

  /**
   * Copies segment from given segmentURI to a final Pinot location.
   * @param segmentMetadata
   * @param segmentUri
   * @param segmentUploaderConfig
   * @return
   */
  public SuccessResponse push(SegmentMetadata segmentMetadata, URI segmentUri, SegmentUploaderConfig segmentUploaderConfig) {
    // Log information about the incoming segment
    logIncomingSegmentInformation(segmentUploaderConfig.getHeaders());

    PinotFS pinotFS = createPinotFS();
    String storageDirectory = _controllerConf.getPinotStorageDir();
    String tableName = segmentMetadata.getTableName();
    String segmentName = segmentMetadata.getName();

    // Check time range
    if (!isSegmentTimeValid(segmentMetadata)) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid segment start/end time for segment: " + segmentName + " of table: " + tableName, Response.Status.NOT_ACCEPTABLE);
    }

    // Constructs permanent home of segment with a unique UUID. We will keep all versions of segments uploaded.
    URI dstUri = PinotStorageUtils.constructSegmentLocation(storageDirectory, segmentMetadata);

    try {
      // Move segment to permanent directory
      pinotFS.copy(segmentUri, dstUri);
    } catch (Exception e) {
      throw new RuntimeException("Could not construct destination URI");
    }

    try {
      // Update zk segment metadata
      pushMetadata(segmentMetadata, dstUri, segmentUploaderConfig);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Caught internal server exception while uploading segment",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    return new SuccessResponse("Successfully uploaded segment: " + segmentName + " of table: " + tableName);
  }

  private void logIncomingSegmentInformation(HttpHeaders headers) {
    if (headers != null) {
      // TODO: Add these headers into open source hadoop jobs
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER));
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.TABLE_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER));
    }
  }

  /**
   * Returns true if:
   * - Segment does not have a start/end time, OR
   * - The start/end time are in a valid range (Jan 01 1971 - Jan 01, 2071)
   * @param metadata Segment metadata
   * @return
   */
  private boolean isSegmentTimeValid(SegmentMetadata metadata) {
    Interval interval = metadata.getTimeInterval();
    if (interval == null) {
      return true;
    }

    long startMillis = interval.getStartMillis();
    long endMillis = interval.getEndMillis();

    if (!TimeUtils.timeValueInValidRange(startMillis) || !TimeUtils.timeValueInValidRange(endMillis)) {
      Date minDate = new Date(TimeUtils.getValidMinTimeMillis());
      Date maxDate = new Date(TimeUtils.getValidMaxTimeMillis());

      LOGGER.error(
          "Invalid start time '{}ms' or end time '{}ms' for segment {}, must be between '{}' and '{}' (timecolumn {}, timeunit {})",
          interval.getStartMillis(), interval.getEndMillis(), metadata.getName(), minDate, maxDate,
          metadata.getTimeColumn(), metadata.getTimeUnit().toString());
      return false;
    }

    return true;
  }

  private String getTableNameWithType(String segmentName, String tableName) {
    if (SegmentName.getSegmentType(segmentName).equals(SegmentName.RealtimeSegmentType.UNSUPPORTED)) {
      // Offline segment
      return TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    } else {
      return TableNameBuilder.REALTIME.tableNameWithType(tableName);
    }
  }

  private String getSegmentVersionUUID(URI segmentUri, PinotFS pinotFS) {
    try {
      URI parentURI = new URI(pinotFS.getParentFile(segmentUri));
      return pinotFS.getName(parentURI);
    } catch (Exception e) {
      LOGGER.error("Could not retrieve parent URI");
      throw new ControllerApplicationException(LOGGER, "Invalid segment location for segment: " + segmentUri, Response.Status.BAD_REQUEST);
    }
  }

  private PinotFS createPinotFS() {
    return _pinotFSFactory.create();
  }

}
