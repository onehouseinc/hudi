/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.Option;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Statistics about a single Hoodie delta log operation.
 *
 * NOTE: This class defines setters to be used by Jackson deserializer (in lieu of using reflection
 *       to set these fields
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("rawtypes")
public class HoodieDeltaWriteStat extends HoodieWriteStat {

  private int logVersion;
  private long logOffset;
  private String baseFile;
  private List<String> logFiles = new ArrayList<>();
  private Map<String, HoodieColumnRangeMetadata<Comparable>> recordsStats = null;

  public void setLogVersion(int logVersion) {
    this.logVersion = logVersion;
  }

  public int getLogVersion() {
    return logVersion;
  }

  public void setLogOffset(long logOffset) {
    this.logOffset = logOffset;
  }

  public long getLogOffset() {
    return logOffset;
  }

  public void setBaseFile(String baseFile) {
    this.baseFile = baseFile;
  }

  public String getBaseFile() {
    return baseFile;
  }

  public void setLogFiles(List<String> logFiles) {
    this.logFiles = logFiles;
  }

  public void addLogFiles(String logFile) {
    logFiles.add(logFile);
  }

  public List<String> getLogFiles() {
    return logFiles;
  }

  public void setRecordsStats(Map<String, HoodieColumnRangeMetadata<Comparable>> stats) {
    recordsStats = stats;
  }

  public Option<Map<String, HoodieColumnRangeMetadata<Comparable>>> getRecordsStats() {
    return Option.ofNullable(recordsStats);
  }

  public void mergeRecordsStats(Map<String, HoodieColumnRangeMetadata<Comparable>> stats) {
    if (recordsStats == null) {
      this.recordsStats = stats;
    } else {
      this.recordsStats = mergeRecordsStats(recordsStats, stats);
    }
  }

  private static Map<String, HoodieColumnRangeMetadata<Comparable>> mergeRecordsStats(
      Map<String, HoodieColumnRangeMetadata<Comparable>> stats1,
      Map<String, HoodieColumnRangeMetadata<Comparable>> stats2) {
    Map<String, HoodieColumnRangeMetadata<Comparable>> mergedStats = new HashMap<>(stats1);
    for (Map.Entry<String, HoodieColumnRangeMetadata<Comparable>> entry : stats2.entrySet()) {
      final String colName = entry.getKey();
      final HoodieColumnRangeMetadata<Comparable> metadata = mergedStats.containsKey(colName)
          ? HoodieColumnRangeMetadata.merge(mergedStats.get(colName), entry.getValue())
          : entry.getValue();
      mergedStats.put(colName, metadata);
    }
    return mergedStats;
  }
}
