/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.util.ReflectionUtils
import org.apache.hudi.{HoodieSparkUtils, SparkAdapterSupport}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis.{MatchInsertIntoStatement, ResolvesToHudiTable, sparkAdapter}
import org.apache.spark.sql.hudi.command._
import org.apache.spark.sql.hudi.command.procedures.{HoodieProcedures, Procedure, ProcedureArgs}
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.util
import scala.collection.mutable.ListBuffer

object HoodieAnalysis extends SparkAdapterSupport {
  type RuleBuilder = SparkSession => Rule[LogicalPlan]

  def customOptimizerRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer(
      // Default rules
    )

    if (HoodieSparkUtils.gteqSpark3_1) {
      val nestedSchemaPruningClass =
        if (HoodieSparkUtils.gteqSpark3_3) {
          "org.apache.spark.sql.execution.datasources.Spark33NestedSchemaPruning"
        } else if (HoodieSparkUtils.gteqSpark3_2) {
          "org.apache.spark.sql.execution.datasources.Spark32NestedSchemaPruning"
        } else {
          // spark 3.1
          "org.apache.spark.sql.execution.datasources.Spark31NestedSchemaPruning"
        }

      val nestedSchemaPruningRule = ReflectionUtils.loadClass(nestedSchemaPruningClass).asInstanceOf[Rule[LogicalPlan]]
      rules += (_ => nestedSchemaPruningRule)
    }

    rules
  }

  def customResolutionRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer(
      // Default rules
      _ => AdaptLogicalRelations()
    )

    if (HoodieSparkUtils.isSpark2) {
      val spark2ResolveReferencesClass = "org.apache.spark.sql.catalyst.analysis.HoodieSpark2Analysis$ResolveReferences"
      val spark2ResolveReferences: RuleBuilder =
        session => ReflectionUtils.loadClass(spark2ResolveReferencesClass, session).asInstanceOf[Rule[LogicalPlan]]

      // NOTE: It's crucial
      rules += spark2ResolveReferences
    }

    if (HoodieSparkUtils.gteqSpark3_2) {
      val dataSourceV2ToV1FallbackClass = "org.apache.spark.sql.hudi.analysis.HoodieDataSourceV2ToV1Fallback"
      val dataSourceV2ToV1Fallback: RuleBuilder =
        session => ReflectionUtils.loadClass(dataSourceV2ToV1FallbackClass, session).asInstanceOf[Rule[LogicalPlan]]

      val spark32PlusResolveReferencesClass = "org.apache.spark.sql.hudi.analysis.HoodieSpark32PlusResolveReferences"
      val spark32PlusResolveReferences: RuleBuilder =
        session => ReflectionUtils.loadClass(spark32PlusResolveReferencesClass, session).asInstanceOf[Rule[LogicalPlan]]

      // NOTE: PLEASE READ CAREFULLY
      //
      // It's critical for this rules to follow in this order; re-ordering this rules might lead to changes in
      // behavior of Spark's analysis phase (for ex, DataSource V2 to V1 fallback might not kick in before other rules,
      // leading to all relations resolving as V2 instead of current expectation of them being resolved as V1)
      rules ++= Seq(dataSourceV2ToV1Fallback, spark32PlusResolveReferences)
    }

    if (HoodieSparkUtils.isSpark3) {
      val resolveAlterTableCommandsClass =
        if (HoodieSparkUtils.gteqSpark3_3) {
          "org.apache.spark.sql.hudi.Spark33ResolveHudiAlterTableCommand"
        } else if (HoodieSparkUtils.gteqSpark3_2) {
          "org.apache.spark.sql.hudi.Spark32ResolveHudiAlterTableCommand"
        } else if (HoodieSparkUtils.gteqSpark3_1) {
          "org.apache.spark.sql.hudi.Spark31ResolveHudiAlterTableCommand"
        } else {
          throw new IllegalStateException("Unsupported Spark version")
        }

      val resolveAlterTableCommands: RuleBuilder =
        session => ReflectionUtils.loadClass(resolveAlterTableCommandsClass, session).asInstanceOf[Rule[LogicalPlan]]

      rules += resolveAlterTableCommands
    }

    rules
  }

  def customPostHocResolutionRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer(
      // Default rules
      session => ResolveImplementations(session),
      _ => StripLogicalRelationAdapters()
    )

    if (HoodieSparkUtils.gteqSpark3_2) {
      val spark3PostHocResolutionClass = "org.apache.spark.sql.hudi.analysis.HoodieSpark32PlusPostAnalysisRule"
      val spark3PostHocResolution: RuleBuilder =
        session => ReflectionUtils.loadClass(spark3PostHocResolutionClass, session).asInstanceOf[Rule[LogicalPlan]]

      rules += spark3PostHocResolution
    }

    rules
  }

  /**
   * This rule wraps [[LogicalRelation]] into [[HoodieLogicalRelationAdapter]] such that all of the
   * default Spark resolution could be applied resolving standard Spark SQL commands like `MERGE INTO`,
   * `INSERT INTO` even though Hudi tables might be carrying meta-fields that have to be ignored during
   * resolution phase.
   *
   * Spark >= 3.2 bears fully-fledged support for meta-fields and such antics are not required for it:
   * we just need to annotate corresponding attributes as "metadata" for Spark to be able to ignore it.
   *
   * In Spark < 3.2 however, this is worked around like following
   *
   * <ol>
   *   <li>[[AdaptLogicalRelations]] wraps around any [[LogicalRelation]] resolving to a target Hudi table
   *   w/ [[HoodieLogicalRelationAdapter]]</li>
   *   <li>Spark resolution rules are executed appropriately resolving [[LogicalPlan]] tree</li>
   *   <li>[[StripLogicalRelationAdapters]] strips away [[HoodieLogicalRelationAdapter]]</li>
   * </ol>
   */
  case class AdaptLogicalRelations() extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan =
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        // NOTE: First we need to check whether this rule had already been applied to
        //       this plan and all [[LogicalRelation]]s of Hudi tables had already been wrapped
        //       (requires no more than just one pass)
        plan.collectFirst {
          case hlr @ HoodieLogicalRelationAdapter(_) => hlr
        } match {
          case Some(_) => plan // no-op
          case None =>
            // NOTE: It's critical to transform the tree in post-order here to make sure this traversal isn't
            //       looping infinitely
            plan.transformUp {
              case lr @ LogicalRelation(_, _, Some(table), _) if sparkAdapter.isHoodieTable(table) =>
                // NOTE: Have to make a copy here, since by default Spark is caching resolved [[LogicalRelation]]s
                HoodieLogicalRelationAdapter(lr.copy())
            }
        }
      }
  }

  /**
   * Please check out scala-doc for [[AdaptLogicalRelations]]
   */
  case class StripLogicalRelationAdapters() extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        plan.transformDown {
          // NOTE: Here we expose full output of the original [[LogicalRelation]] again (including meta-fields)
          //       to make sure if meta-fields were accessed by some operators upstream (t/h metadata-output
          //       resolution) these are still accessible.
          //       At this stage, we've already cleared the analysis (resolution) phase, therefore it's safe to do so
          case HoodieLogicalRelationAdapter(lr: LogicalRelation) => lr
        }
      }
    }
  }

  private[sql] object MatchInsertIntoStatement {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] =
      sparkAdapter.getCatalystPlanUtils.unapplyInsertIntoStatement(plan)
  }

  private[sql] object ResolvesToHudiTable {
    def unapply(plan: LogicalPlan): Option[CatalogTable] =
      sparkAdapter.resolveHoodieTable(plan)
  }

  private[sql] def failAnalysis(msg: String): Nothing = {
    throw new AnalysisException(msg)
  }
}

/**
 * Rule converting *fully-resolved* Spark SQL plans into Hudi's custom implementations
 */
case class ResolveImplementations(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Convert to MergeIntoHoodieTableCommand
      case mit @ MergeIntoTable(target @ ResolvesToHudiTable(_), _, _, _, _) if mit.resolved =>
        MergeIntoHoodieTableCommand(mit)

      // Convert to UpdateHoodieTableCommand
      case ut @ UpdateTable(plan @ ResolvesToHudiTable(_), _, _) if ut.resolved =>
        UpdateHoodieTableCommand(ut)

      // Convert to DeleteHoodieTableCommand
      case dft @ DeleteFromTable(plan @ ResolvesToHudiTable(_), _) if dft.resolved =>
        DeleteHoodieTableCommand(dft)

      // Convert to InsertIntoHoodieTableCommand
      case iis @ MatchInsertIntoStatement(relation @ ResolvesToHudiTable(_), partition, query, overwrite, _) if query.resolved =>
        relation match {
          // NOTE: In Spark >= 3.2, Hudi relations will be resolved as [[DataSourceV2Relation]]s by default;
          //       However, currently, fallback will be applied downgrading them to V1 relations, hence
          //       we need to check whether we could proceed here, or has to wait until fallback rule kicks in
          case lr: LogicalRelation => new InsertIntoHoodieTableCommand(lr, query, partition, overwrite)
          case _ => iis
        }

      // Convert to CreateHoodieTableAsSelectCommand
      case ct @ CreateTable(table, mode, Some(query))
        if sparkAdapter.isHoodieTable(table) && ct.query.forall(_.resolved) =>
          CreateHoodieTableAsSelectCommand(table, mode, query)

      // Convert to CompactionHoodieTableCommand
      case ct @ CompactionTable(plan @ ResolvesToHudiTable(table), operation, options) if ct.resolved =>
        CompactionHoodieTableCommand(table, operation, options)

      // Convert to CompactionHoodiePathCommand
      case cp @ CompactionPath(path, operation, options) if cp.resolved =>
        CompactionHoodiePathCommand(path, operation, options)

      // Convert to CompactionShowOnTable
      case csot @ CompactionShowOnTable(plan @ ResolvesToHudiTable(table), limit) if csot.resolved =>
        CompactionShowHoodieTableCommand(table, limit)

      // Convert to CompactionShowHoodiePathCommand
      case csop @ CompactionShowOnPath(path, limit) if csop.resolved =>
        CompactionShowHoodiePathCommand(path, limit)

      // Convert to HoodieCallProcedureCommand
      case c @ CallCommand(_, _) =>
        val procedure: Option[Procedure] = loadProcedure(c.name)
        val input = buildProcedureArgs(c.args)
        if (procedure.nonEmpty) {
          CallProcedureHoodieCommand(procedure.get, input)
        } else {
          c
        }

      // Convert to CreateIndexCommand
      case ci @ CreateIndex(plan @ ResolvesToHudiTable(table), indexName, indexType, ignoreIfExists, columns, options, output) =>
        // TODO need to resolve columns
        CreateIndexCommand(table, indexName, indexType, ignoreIfExists, columns, options, output)

      // Convert to DropIndexCommand
      case di @ DropIndex(plan @ ResolvesToHudiTable(table), indexName, ignoreIfNotExists, output) if di.resolved =>
        DropIndexCommand(table, indexName, ignoreIfNotExists, output)

      // Convert to ShowIndexesCommand
      case si @ ShowIndexes(plan @ ResolvesToHudiTable(table), output) if si.resolved =>
        ShowIndexesCommand(table, output)

      // Covert to RefreshCommand
      case ri @ RefreshIndex(plan @ ResolvesToHudiTable(table), indexName, output) if ri.resolved =>
        RefreshIndexCommand(table, indexName, output)

      // Rewrite the CreateDataSourceTableCommand to CreateHoodieTableCommand
      case CreateDataSourceTableCommand(table, ignoreIfExists) if sparkAdapter.isHoodieTable(table) =>
        CreateHoodieTableCommand(table, ignoreIfExists)

      // Rewrite the DropTableCommand to DropHoodieTableCommand
      case DropTableCommand(tableName, ifExists, false, purge) if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
        DropHoodieTableCommand(tableName, ifExists, false, purge)

      // Rewrite the AlterTableDropPartitionCommand to AlterHoodieTableDropPartitionCommand
      case AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
        if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
          AlterHoodieTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)

      // Rewrite the AlterTableRenameCommand to AlterHoodieTableRenameCommand
      // Rewrite the AlterTableAddColumnsCommand to AlterHoodieTableAddColumnsCommand
      case AlterTableAddColumnsCommand(tableId, colsToAdd) if sparkAdapter.isHoodieTable(tableId, sparkSession) =>
        AlterHoodieTableAddColumnsCommand(tableId, colsToAdd)

      // Rewrite the AlterTableRenameCommand to AlterHoodieTableRenameCommand
      case AlterTableRenameCommand(oldName, newName, isView) if !isView && sparkAdapter.isHoodieTable(oldName, sparkSession) =>
        AlterHoodieTableRenameCommand(oldName, newName, isView)

      // Rewrite the AlterTableChangeColumnCommand to AlterHoodieTableChangeColumnCommand
      case AlterTableChangeColumnCommand(tableName, columnName, newColumn) if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
        AlterHoodieTableChangeColumnCommand(tableName, columnName, newColumn)

      // SPARK-34238: the definition of ShowPartitionsCommand has been changed in Spark3.2.
      // Match the class type instead of call the `unapply` method.
      case s: ShowPartitionsCommand if sparkAdapter.isHoodieTable(s.tableName, sparkSession) =>
        ShowHoodieTablePartitionsCommand(s.tableName, s.spec)

      // Rewrite TruncateTableCommand to TruncateHoodieTableCommand
      case TruncateTableCommand(tableName, partitionSpec) if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
        TruncateHoodieTableCommand(tableName, partitionSpec)

      // Rewrite RepairTableCommand to RepairHoodieTableCommand
      case r if sparkAdapter.getCatalystPlanUtils.isRepairTable(r) =>
        val (tableName, enableAddPartitions, enableDropPartitions, cmd) = sparkAdapter.getCatalystPlanUtils.getRepairTableChildren(r).get
        if (sparkAdapter.isHoodieTable(tableName, sparkSession)) {
          RepairHoodieTableCommand(tableName, enableAddPartitions, enableDropPartitions, cmd)
        } else {
          r
        }

      case _ => plan
    }
  }

  private def loadProcedure(name: Seq[String]): Option[Procedure] = {
    val procedure: Option[Procedure] = if (name.nonEmpty) {
      val builder = HoodieProcedures.newBuilder(name.last)
      if (builder != null) {
        Option(builder.build)
      } else {
        throw new AnalysisException(s"procedure: ${name.last} is not exists")
      }
    } else {
      None
    }
    procedure
  }

  private def buildProcedureArgs(exprs: Seq[CallArgument]): ProcedureArgs = {
    val values = new Array[Any](exprs.size)
    var isNamedArgs: Boolean = false
    val map = new util.LinkedHashMap[String, Int]()
    for (index <- exprs.indices) {
      exprs(index) match {
        case expr: NamedArgument =>
          map.put(expr.name, index)
          values(index) = expr.expr.eval()
          isNamedArgs = true
        case _ =>
          map.put(index.toString, index)
          values(index) = exprs(index).expr.eval()
          isNamedArgs = false
      }
    }
    ProcedureArgs(isNamedArgs, map, new GenericInternalRow(values))
  }
}
