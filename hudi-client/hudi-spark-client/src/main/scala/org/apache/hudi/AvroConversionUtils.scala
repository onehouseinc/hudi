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

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}
import org.apache.avro.{JsonProperties, Schema}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AvroConversionUtils {

  /**
   * Creates converter to transform Avro payload into Spark's Catalyst one
   *
   * @param rootAvroType Avro [[Schema]] to be transformed from
   * @param rootCatalystType Catalyst [[StructType]] to be transformed into
   * @return converter accepting Avro payload and transforming it into a Catalyst one (in the form of [[InternalRow]])
   */
  def createAvroToRowConverter(rootAvroType: Schema, rootCatalystType: StructType): GenericRecord => Option[InternalRow] =
    record => HoodieAvroDeserializer(rootAvroType, rootCatalystType)
      .deserializeData(record)
      .map(_.asInstanceOf[InternalRow])

  /**
   * Creates converter to transform Catalyst payload into Avro one
   *
   * @param rootCatalystType Catalyst [[StructType]] to be transformed from
   * @param rootAvroType Avro [[Schema]] to be transformed into
   * @param nullable whether Avro record is nullable
   * @return converter accepting Catalyst payload (in the form of [[InternalRow]]) and transforming it into an Avro one
   */
  def createRowToAvroConverter(rootCatalystType: StructType, rootAvroType: Schema, nullable: Boolean): InternalRow => GenericRecord = {
    row => HoodieAvroSerializer(rootCatalystType, rootAvroType, nullable)
      .serialize(row)
      .asInstanceOf[GenericRecord]
  }

  def createDataFrame(rdd: RDD[GenericRecord], schemaStr: String, ss: SparkSession): Dataset[Row] = {
    if (rdd.isEmpty()) {
      ss.emptyDataFrame
    } else {
      ss.createDataFrame(rdd.mapPartitions { records =>
        if (records.isEmpty) Iterator.empty
        else {
          val schema = new Schema.Parser().parse(schemaStr)
          val dataType = convertAvroSchemaToStructType(schema)
          val converter = AvroConversionHelper.createConverterToRow(schema, dataType)
          records.map { r => converter(r) }
        }
      }, convertAvroSchemaToStructType(new Schema.Parser().parse(schemaStr)))
    }
  }

  /**
    *
    * Returns avro schema from spark StructType.
    *
    * @param structType       Dataframe Struct Type.
    * @param structName       Avro record name.
    * @param recordNamespace  Avro record namespace.
    * @return                 Avro schema corresponding to given struct type.
    */
  def convertStructTypeToAvroSchema(structType: DataType,
                                    structName: String,
                                    recordNamespace: String): Schema = {
    getAvroSchemaWithDefaults(SchemaConverters.toAvroType(structType, nullable = false, structName, recordNamespace))
  }

  /**
    *
    * Method to add default value of null to nullable fields in given avro schema
    *
    * @param schema     input avro schema
    * @return           Avro schema with null default set to nullable fields
    */
  def getAvroSchemaWithDefaults(schema: Schema): Schema = {

    schema.getType match {
      case Schema.Type.RECORD => {

        val modifiedFields = schema.getFields.map(field => {
          val newSchema = getAvroSchemaWithDefaults(field.schema())
          field.schema().getType match {
            case Schema.Type.UNION => {
              val innerFields = newSchema.getTypes
              val containsNullSchema = innerFields.foldLeft(false)((nullFieldEncountered, schema) => nullFieldEncountered | schema.getType == Schema.Type.NULL)
              if(containsNullSchema) {
                // Need to re shuffle the fields in list because to set null as default, null schema must be head in union schema
                val restructuredNewSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL)) ++ innerFields.filter(innerSchema => !(innerSchema.getType == Schema.Type.NULL)))
                new Schema.Field(field.name(), restructuredNewSchema, field.doc(), JsonProperties.NULL_VALUE)
              } else {
                new Schema.Field(field.name(), newSchema, field.doc(), field.defaultVal())
              }
            }
            case _ => new Schema.Field(field.name(), newSchema, field.doc(), field.defaultVal())
          }
        }).toList
        Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError, modifiedFields)
      }

      case Schema.Type.UNION => {
        Schema.createUnion(schema.getTypes.map(innerSchema => getAvroSchemaWithDefaults(innerSchema)))
      }

      case Schema.Type.MAP => {
        Schema.createMap(getAvroSchemaWithDefaults(schema.getValueType))
      }

      case Schema.Type.ARRAY => {
        Schema.createArray(getAvroSchemaWithDefaults(schema.getElementType))
      }

      case _ => schema
    }
  }

  def convertAvroSchemaToStructType(avroSchema: Schema): StructType = {
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
  }

  def buildAvroRecordBySchema(record: IndexedRecord,
                              requiredSchema: Schema,
                              requiredPos: Seq[Int],
                              recordBuilder: GenericRecordBuilder): GenericRecord = {
    val requiredFields = requiredSchema.getFields.asScala
    assert(requiredFields.length == requiredPos.length)
    val positionIterator = requiredPos.iterator
    requiredFields.foreach(f => recordBuilder.set(f, record.get(positionIterator.next())))
    recordBuilder.build()
  }

  def getAvroRecordNameAndNamespace(tableName: String): (String, String) = {
    val name = HoodieAvroUtils.sanitizeName(tableName)
    (s"${name}_record", s"hoodie.${name}")
  }
}
