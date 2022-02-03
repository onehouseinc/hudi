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

package org.apache.hudi.hadoop.realtime;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link FileSplit} implementation that holds
 * <ol>
 *   <li>Split corresponding to the base file</li>
 *   <li>List of {@link HoodieLogFile} that holds the delta to be merged (upon reading)</li>
 * </ol>
 *
 * This split is correspondent to a single file-slice in the Hudi terminology.
 *
 * NOTE: If you're adding fields here you need to make sure that you appropriately de-/serialize them
 *       in {@link #readFromInput(DataInput)} and {@link #writeToOutput(DataOutput)}
 */
public class HoodieRealtimeFileSplit extends FileSplit implements RealtimeSplit {
  /**
   * List of delta log-files holding updated records
   */
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();
  /**
   * Latest commit instant available at the time of the query in which all of the files
   * pertaining to this split are represented
   */
  private String maxCommitTime;
  /**
   * Base file's path
   */
  private String basePath;
  /**
   * Virtual key configuration of the table this split belongs to
   */
  private Option<HoodieVirtualKeyInfo> virtualKeyInfo = Option.empty();

  public HoodieRealtimeFileSplit() {}

  public HoodieRealtimeFileSplit(FileSplit baseSplit, String basePath, List<HoodieLogFile> deltaLogFiles, String maxCommitTime,
                                 Option<HoodieVirtualKeyInfo> virtualKeyInfo)
      throws IOException {
    super(baseSplit.getPath(), baseSplit.getStart(), baseSplit.getLength(), baseSplit.getLocations());
    this.deltaLogFiles = deltaLogFiles;
    this.maxCommitTime = maxCommitTime;
    this.basePath = basePath;
    this.virtualKeyInfo = virtualKeyInfo;
  }

  public List<HoodieLogFile> getDeltaLogFiles() {
    return deltaLogFiles;
  }

  @Override
  public void setDeltaLogFiles(List<HoodieLogFile> deltaLogFiles) {
    this.deltaLogFiles = deltaLogFiles;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  public String getBasePath() {
    return basePath;
  }

  @Override
  public void setVirtualKeyInfo(Option<HoodieVirtualKeyInfo> virtualKeyInfo) {
    this.virtualKeyInfo = virtualKeyInfo;
  }

  @Override
  public Option<HoodieVirtualKeyInfo> getVirtualKeyInfo() {
    return virtualKeyInfo;
  }

  public void setMaxCommitTime(String maxCommitTime) {
    this.maxCommitTime = maxCommitTime;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    writeToOutput(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    readFromInput(in);
  }

  @Override
  public String toString() {
    return "HoodieRealtimeFileSplit{DataPath=" + getPath() + ", deltaLogPaths=" + getDeltaLogPaths()
        + ", maxCommitTime='" + maxCommitTime + '\'' + ", basePath='" + basePath + '\'' + '}';
  }
}
