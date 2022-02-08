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

package org.apache.hudi.table.action.commit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.Iterator;

public class SparkMergeHelper<T extends HoodieRecordPayload> extends BaseMergeHelper<T, JavaRDD<HoodieRecord<T>>,
    JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private SparkMergeHelper() {
  }

  private static class MergeHelperHolder {
    private static final SparkMergeHelper SPARK_MERGE_HELPER = new SparkMergeHelper();
  }

  public static SparkMergeHelper newInstance() {
    return SparkMergeHelper.MergeHelperHolder.SPARK_MERGE_HELPER;
  }

  @Override
  public void runMerge(HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                       HoodieMergeHandle<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> upsertHandle) throws IOException {
    final boolean externalSchemaTransformation = table.getConfig().shouldUseExternalSchemaTransformation();
    Configuration cfgForHoodieFile = new Configuration(table.getHadoopConf());
    HoodieMergeHandle<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> mergeHandle = upsertHandle;
    HoodieBaseFile baseFile = mergeHandle.baseFileForMerge();

    Schema readerSchema;
    Schema writerSchema = mergeHandle.getWriterSchemaWithMetaFields();

    if (externalSchemaTransformation || baseFile.getBootstrapBaseFile().isPresent()) {
      readerSchema = HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), mergeHandle.getOldFilePath()).getSchema();
    } else {
      readerSchema = writerSchema;
    }

    BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
    HoodieFileReader reader = HoodieFileReaderFactory.getFileReader(cfgForHoodieFile, mergeHandle.getOldFilePath());
    try {
      final Iterator<HoodieRecord> readerIterator;
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        readerIterator = getMergingIterator(table, mergeHandle, baseFile, reader, readerSchema, externalSchemaTransformation);
      } else {
        readerIterator = reader.getRecordIterator(readerSchema, createHoodieRecordMapper(table));
      }

      wrapper = new SparkBoundedInMemoryExecutor(table.getConfig(), readerIterator,
          new UpdateHandler(mergeHandle), record -> {
        if (!externalSchemaTransformation) {
          return record;
        }
        return transformRecordBasedOnNewSchema((GenericRecord) record, writerSchema);
      });
      wrapper.execute();
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      if (reader != null) {
        reader.close();
      }
      mergeHandle.close();
      if (null != wrapper) {
        wrapper.shutdownNow();
      }
    }
  }
}
