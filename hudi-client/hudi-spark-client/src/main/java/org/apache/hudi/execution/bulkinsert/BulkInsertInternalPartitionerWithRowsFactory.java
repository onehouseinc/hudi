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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A factory to generate built-in partitioner to repartition input Rows into at least
 * expected number of output spark partitions for bulk insert operation.
 */
public abstract class BulkInsertInternalPartitionerWithRowsFactory {

  public static BulkInsertPartitioner<Dataset<Row>> get(BulkInsertSortMode bulkInsertMode, HoodieTableConfig tableConfig) {
    switch (bulkInsertMode) {
      case NONE:
        return new NonSortPartitionerWithRows();
      case GLOBAL_SORT:
        return new GlobalSortPartitionerWithRows();
      case PARTITION_SORT:
        return new PartitionSortPartitionerWithRows(tableConfig);
      case PARTITION_NO_SORT:
        return new PartitionNoSortPartitionerWithRows(tableConfig);
      default:
        throw new UnsupportedOperationException("The bulk insert sort mode \"" + bulkInsertMode.name() + "\" is not supported.");
    }
  }
}
