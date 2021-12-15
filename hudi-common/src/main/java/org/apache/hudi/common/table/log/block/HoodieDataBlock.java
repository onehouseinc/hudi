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

package org.apache.hudi.common.table.log.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.fs.inline.InLineFileSystem;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * DataBlock contains a list of records serialized using formats compatible with the base file format.
 * For each base file format there is a corresponding DataBlock format.
 *
 * The Datablock contains:
 *   1. Data Block version
 *   2. Total number of records in the block
 *   3. Actual serialized content of the records
 */
public abstract class HoodieDataBlock extends HoodieLogBlock {

  private List<IndexedRecord> records;
  private final String keyFieldRef;

  protected final Schema readerSchema;

  public HoodieDataBlock(
      @Nonnull Map<HeaderMetadataType, String> logBlockHeader,
      @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
      @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation,
      @Nonnull Option<byte[]> content,
      @Nullable FSDataInputStream inputStream,
      boolean readBlockLazily) {
    super(logBlockHeader, logBlockFooter, blockContentLocation, content, inputStream, readBlockLazily);
    this.records = null;
    this.readerSchema = null;
    this.keyFieldRef = HoodieRecord.RECORD_KEY_METADATA_FIELD;
  }

  public HoodieDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header,
                         @Nonnull Map<HeaderMetadataType, String> footer, String keyFieldRef) {
    super(header, footer, Option.empty(), Option.empty(), null, false);
    this.records = records;
    // If no reader-schema has been provided assume writer-schema as one
    this.readerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    this.keyFieldRef = keyFieldRef;
  }

  protected HoodieDataBlock(Option<byte[]> content, @Nonnull FSDataInputStream inputStream, boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation, Schema readerSchema,
                            @Nonnull Map<HeaderMetadataType, String> headers, @Nonnull Map<HeaderMetadataType,
      String> footer, String keyFieldRef) {
    super(headers, footer, blockContentLocation, content, inputStream, readBlockLazily);
    this.records = null;
    this.readerSchema = readerSchema;
    this.keyFieldRef = keyFieldRef;
  }

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat - Data block type
   * @param recordList         - List of records that goes in the data block
   * @param header             - data block header
   * @return Data block of the requested type.
   */
  public static HoodieLogBlock getBlock(HoodieLogBlockType logDataBlockFormat, List<IndexedRecord> recordList,
                                        Map<HeaderMetadataType, String> header) {
    return getBlock(logDataBlockFormat, recordList, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat - Data block type
   * @param recordList         - List of records that goes in the data block
   * @param header             - data block header
   * @param keyField           - FieldId to get the key from the records
   * @return Data block of the requested type.
   */
  public static HoodieLogBlock getBlock(HoodieLogBlockType logDataBlockFormat, List<IndexedRecord> recordList,
                                        Map<HeaderMetadataType, String> header, String keyField) {
    switch (logDataBlockFormat) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(recordList, header, keyField);
      case HFILE_DATA_BLOCK:
        return new HoodieHFileDataBlock(recordList, header, keyField);
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(recordList, header, keyField);
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " not implemented");
    }
  }

  @Override
  public byte[] getContentBytes() throws IOException {
    // In case this method is called before realizing records from content
    Option<byte[]> content = getContent();

    if (content.isPresent()) {
      return content.get();
    } else if (readBlockLazily && records == null) {
      // read block lazily
      readRecordsFromContent();
    }

    return serializeRecords(records);
  }

  public final List<IndexedRecord> getRecords() {
    if (records == null) {
      try {
        // in case records are absent, read content lazily and then convert to IndexedRecords
        readRecordsFromContent();
      } catch (IOException io) {
        throw new HoodieIOException("Unable to convert content bytes to records", io);
      }
    }
    return records;
  }

  public abstract HoodieLogBlockType getBlockType();

  /**
   * Batch get of keys of interest. Implementation can choose to either do full scan and return matched entries or
   * do a seek based parsing and return matched entries.
   * @param keys keys of interest.
   * @return List of IndexedRecords for the keys of interest.
   * @throws IOException
   */
  public List<IndexedRecord> getRecords(List<String> keys) throws IOException {
    throw new UnsupportedOperationException("On demand batch get based on interested keys not supported");
  }

  public Schema getSchema() {
    return readerSchema;
  }

  protected void readRecordsFromContent() throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    records = deserializeRecords(getContent().get());
    // Free up content to be GC'd, deflate
    deflate();
  }

  protected abstract byte[] serializeRecords(List<IndexedRecord> records) throws IOException;

  protected abstract List<IndexedRecord> deserializeRecords(byte[] content) throws IOException;

  protected Option<Schema.Field> getKeyField(Schema schema) {
    return Option.ofNullable(schema.getField(keyFieldRef));
  }

  protected Option<String> getRecordKey(IndexedRecord record) {
    return getKeyField(record.getSchema()).map(keyField -> record.get(keyField.pos()).toString());
  }
}
