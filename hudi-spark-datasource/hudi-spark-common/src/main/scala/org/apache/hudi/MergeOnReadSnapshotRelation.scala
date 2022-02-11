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

package org.apache.hudi

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieBaseRelation.createBaseFileReader
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionedFile}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConverters._

case class HoodieMergeOnReadFileSplit(dataFile: Option[PartitionedFile],
                                      logPaths: Option[List[HoodieLogFile]],
                                      latestCommit: String,
                                      tablePath: String,
                                      maxCompactionMemoryInBytes: Long,
                                      mergeType: String)

case class HoodieMergeOnReadTableState(schemas: HoodieTableSchemas,
                                       hoodieRealtimeFileSplits: List[HoodieMergeOnReadFileSplit],
                                       preCombineField: Option[String],
                                       recordKeyField: String)

class MergeOnReadSnapshotRelation(sqlContext: SQLContext,
                                  optParams: Map[String, String],
                                  val userSchema: Option[StructType],
                                  val globPaths: Seq[Path],
                                  val metaClient: HoodieTableMetaClient)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema) {

  private val conf = sqlContext.sparkContext.hadoopConfiguration
  private val jobConf = new JobConf(conf)
  // use schema from latest metadata, if not present, read schema from the data file
  private val schemaResolver = new TableSchemaResolver(metaClient)
  private lazy val tableAvroSchema = {
    try {
      schemaResolver.getTableAvroSchema(metaClient.getTableConfig.populateMetaFields)
    } catch {
      case _: Throwable => // If there is no commit in the table, we cann't get the schema
        // with schemaUtil, use the userSchema instead.
        SchemaConverters.toAvroType(userSchema)
    }
  }

  private val mergeType = optParams.getOrElse(
    DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  private val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(jobConf)

  private val preCombineField = {
    val preCombineFieldFromTableConfig = metaClient.getTableConfig.getPreCombineField
    if (preCombineFieldFromTableConfig != null) {
      Some(preCombineFieldFromTableConfig)
    } else {
      // get preCombineFiled from the options if this is a old table which have not store
      // the field to hoodie.properties
      optParams.get(DataSourceReadOptions.READ_PRE_COMBINE_FIELD.key)
    }
  }
  private val recordKeyField = metaClient.getTableConfig.getRecordKeyFieldProp

  override def needConversion: Boolean = false

  private val specifiedQueryInstant = optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
    .map(HoodieSqlCommonUtils.formatQueryInstant)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.debug(s" buildScan requiredColumns = ${requiredColumns.mkString(",")}")
    log.debug(s" buildScan filters = ${filters.mkString(",")}")

    // NOTE: In case list of requested columns doesn't contain the Primary Key one, we
    //       have to add it explicitly so that
    //          - Merging could be performed correctly
    //          - In case 0 columns are to be fetched (for ex, when doing {@code count()} on Spark's [[Dataset]],
    //          Spark still fetches all the rows to execute the query correctly
    val fetchedColumns: Array[String] =
      requiredColumns ++ (if (requiredColumns.contains(recordKeyField)) Seq() else Seq(recordKeyField))

    val (requiredAvroSchema, requiredStructSchema) =
      HoodieSparkUtils.getRequiredSchema(tableAvroSchema, fetchedColumns)
    val fileIndex = buildFileIndex(filters)
    val tableSchemas = HoodieTableSchemas(
      tableSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = requiredStructSchema,
      tableAvroSchema = tableAvroSchema.toString,
      requiredAvroSchema = requiredAvroSchema.toString
    )
    val tableState = HoodieMergeOnReadTableState(tableSchemas, fileIndex, preCombineField, recordKeyField)
    val fullSchemaParquetReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      tableSchemas = tableSchemas,
      filters = filters,
      options = optParams,
      hadoopConf = conf
    )
    val requiredSchemaParquetReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      tableSchemas = tableSchemas,
      filters = filters,
      options = optParams,
      hadoopConf = conf
    )

    val rdd = new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      jobConf,
      fullSchemaParquetReader,
      requiredSchemaParquetReader,
      tableState
    )
    rdd.asInstanceOf[RDD[Row]]
  }

  def buildFileIndex(filters: Array[Filter]): List[HoodieMergeOnReadFileSplit] = {
    if (globPaths.nonEmpty) {
      // Load files from the global paths if it has defined to be compatible with the original mode
      val inMemoryFileIndex = HoodieSparkUtils.createInMemoryFileIndex(sqlContext.sparkSession, globPaths)
      val fsView = new HoodieTableFileSystemView(metaClient,
        // file-slice after pending compaction-requested instant-time is also considered valid
        metaClient.getCommitsAndCompactionTimeline.filterCompletedAndCompactionInstants,
        inMemoryFileIndex.allFiles().toArray)
      val partitionPaths = fsView.getLatestBaseFiles.iterator().asScala.toList.map(_.getFileStatus.getPath.getParent)


      if (partitionPaths.isEmpty) { // If this an empty table, return an empty split list.
        List.empty[HoodieMergeOnReadFileSplit]
      } else {
        val lastInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants.lastInstant()
        if (!lastInstant.isPresent) { // Return empty list if the table has no commit
          List.empty
        } else {
          val queryInstant = specifiedQueryInstant.getOrElse(lastInstant.get().getTimestamp)
          val baseAndLogsList = HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(conf, partitionPaths.asJava).asScala
          val fileSplits = baseAndLogsList.map(kv => {
            val baseFile = kv.getLeft
            val logPaths = if (kv.getRight.isEmpty) Option.empty else Option(kv.getRight.asScala.toList)

            val baseDataPath = if (baseFile.isPresent) {
              Some(PartitionedFile(
                InternalRow.empty,
                MergeOnReadSnapshotRelation.getFilePath(baseFile.get.getFileStatus.getPath),
                0, baseFile.get.getFileLen)
              )
            } else {
              None
            }
            HoodieMergeOnReadFileSplit(baseDataPath, logPaths, queryInstant,
              metaClient.getBasePath, maxCompactionMemoryInBytes, mergeType)
          }).toList
          fileSplits
        }
      }
    } else {
      // Load files by the HoodieFileIndex.
      val hoodieFileIndex = HoodieFileIndex(sqlContext.sparkSession, metaClient,
        Some(tableStructSchema), optParams, FileStatusCache.getOrCreate(sqlContext.sparkSession))

      // Get partition filter and convert to catalyst expression
      val partitionColumns = hoodieFileIndex.partitionSchema.fieldNames.toSet
      val partitionFilters = filters.filter(f => f.references.forall(p => partitionColumns.contains(p)))
      val partitionFilterExpression =
        HoodieSparkUtils.convertToCatalystExpressions(partitionFilters, tableStructSchema)
      val convertedPartitionFilterExpression =
        HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilterExpression.toSeq)

      // If convert success to catalyst expression, use the partition prune
      val fileSlices = if (convertedPartitionFilterExpression.nonEmpty) {
        hoodieFileIndex.listFileSlices(convertedPartitionFilterExpression)
      } else {
        hoodieFileIndex.listFileSlices(Seq.empty[Expression])
      }

      if (fileSlices.isEmpty) {
        // If this an empty table, return an empty split list.
        List.empty[HoodieMergeOnReadFileSplit]
      } else {
        val fileSplits = fileSlices.values.flatten.map(fileSlice => {
          val latestInstant = metaClient.getActiveTimeline.getCommitsTimeline
            .filterCompletedInstants.lastInstant().get().getTimestamp
          val queryInstant = specifiedQueryInstant.getOrElse(latestInstant)

          val partitionedFile = if (fileSlice.getBaseFile.isPresent) {
            val baseFile = fileSlice.getBaseFile.get()
            val baseFilePath = MergeOnReadSnapshotRelation.getFilePath(baseFile.getFileStatus.getPath)
            Option(PartitionedFile(InternalRow.empty, baseFilePath, 0, baseFile.getFileLen))
          } else {
            Option.empty
          }

          val logPaths = fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList
          val logPathsOptional = if (logPaths.isEmpty) Option.empty else Option(logPaths)

          HoodieMergeOnReadFileSplit(partitionedFile, logPathsOptional, queryInstant, metaClient.getBasePath,
            maxCompactionMemoryInBytes, mergeType)
        }).toList
        fileSplits
      }
    }
  }
}

object MergeOnReadSnapshotRelation {

  def getFilePath(path: Path): String = {
    // Here we use the Path#toUri to encode the path string, as there is a decode in
    // ParquetFileFormat#buildReaderWithPartitionValues in the spark project when read the table
    // .So we should encode the file path here. Otherwise, there is a FileNotException throw
    // out.
    // For example, If the "pt" is the partition path field, and "pt" = "2021/02/02", If
    // we enable the URL_ENCODE_PARTITIONING and write data to hudi table.The data path
    // in the table will just like "/basePath/2021%2F02%2F02/xxxx.parquet". When we read
    // data from the table, if there are no encode for the file path,
    // ParquetFileFormat#buildReaderWithPartitionValues will decode it to
    // "/basePath/2021/02/02/xxxx.parquet" witch will result to a FileNotException.
    // See FileSourceScanExec#createBucketedReadRDD in spark project which do the same thing
    // when create PartitionedFile.
    path.toUri.toString
  }

}
