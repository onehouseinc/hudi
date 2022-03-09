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

package org.apache.spark.sql.hudi

import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.index.columnstats.ColumnStatsIndexHelper.{getMaxColumnNameFor, getMinColumnNameFor, getNumNullsColumnNameFor}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualNullSafe, EqualTo, Expression, ExtractValue, GetStructField, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, StartsWith, SubqueryExpression}
import org.apache.spark.sql.catalyst.trees.TreePattern.ATTRIBUTE_REFERENCE
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.ColumnStatsExpressionUtils.{SingleAttributeExpression, genColMaxValueExpr, genColMinValueExpr, genColNumNullsExpr, genColumnOnlyValuesEqualToExpression, genColumnValuesEqualToExpression, isSimpleExpression, swapAttributeRefInExpr}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object DataSkippingUtils extends Logging {

  /**
   * Translates provided {@link filterExpr} into corresponding filter-expression for column-stats index index table
   * to filter out candidate files that would hold records matching the original filter
   *
   * @param dataTableFilterExpr source table's query's filter expression
   * @param indexSchema index table schema
   * @return filter for column-stats index's table
   */
  def translateIntoColumnStatsIndexFilterExpr(dataTableFilterExpr: Expression, indexSchema: StructType): Expression = {
    try {
      createColumnStatsIndexFilterExprInternal(dataTableFilterExpr, indexSchema)
    } catch {
      case e: AnalysisException =>
        logDebug(s"Failed to translated provided data table filter expr into column stats one ($dataTableFilterExpr)", e)
        throw e
    }
  }

  private def createColumnStatsIndexFilterExprInternal(dataTableFilterExpr: Expression, indexSchema: StructType): Expression = {
    // Try to transform original Source Table's filter expression into
    // Column-Stats Index filter expression
    tryComposeIndexFilterExpr(dataTableFilterExpr, indexSchema) match {
      case Some(e) => e
      // NOTE: In case we can't transform source filter expression, we fallback
      // to {@code TrueLiteral}, to essentially avoid pruning any indexed files from scanning
      case None => TrueLiteral
    }
  }

  private def tryComposeIndexFilterExpr(sourceExpr: Expression, indexSchema: StructType): Option[Expression] = {
    sourceExpr match {
      // Filter "expr(colA) = B" and "B = expr(colA)"
      // Translates to "(expr(colA_minValue) <= B) AND (B <= expr(colA_maxValue))" condition for index lookup
      case EqualTo(sourceExpr @ SingleAttributeExpression(attrRef), valueExpr: Expression) if isSimpleExpression(valueExpr) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            // NOTE: Since we're supporting (almost) arbitrary expressions of the form `f(colA) = B`, we have to
            //       appropriately translate such original expression targeted at Data Table, to corresponding
            //       expression targeted at Column Stats Index Table. For that, we take original expression holding
            //       [[AttributeReference]] referring to the Data Table, and swap it w/ expression referring to
            //       corresponding column in the Column Stats Index
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            genColumnValuesEqualToExpression(colName, valueExpr, targetExprBuilder)
          }

      case EqualTo(valueExpr: Expression, sourceExpr @ SingleAttributeExpression(attrRef)) if isSimpleExpression(valueExpr) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            genColumnValuesEqualToExpression(colName, valueExpr, targetExprBuilder)
          }

      // Filter "expr(colA) != B" and "B != expr(colA)"
      // Translates to "NOT(expr(colA_minValue) = B AND expr(colA_maxValue) = B)"
      // NOTE: This is NOT an inversion of `colA = b`, instead this filter ONLY excludes files for which `colA = B`
      //       holds true
      case Not(EqualTo(sourceExpr @ SingleAttributeExpression(attrRef), value: Expression)) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            Not(genColumnOnlyValuesEqualToExpression(colName, value, targetExprBuilder))
          }

      case Not(EqualTo(value: Expression, sourceExpr @ SingleAttributeExpression(attrRef))) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            Not(genColumnOnlyValuesEqualToExpression(colName, value, targetExprBuilder))
          }

      // Filter "colA = null"
      // Translates to "colA_num_nulls = null" for index lookup
      case EqualNullSafe(attrRef: AttributeReference, litNull @ Literal(null, _)) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map(colName => EqualTo(genColNumNullsExpr(colName), litNull))

      // Filter "expr(colA) < B" and "B > expr(colA)"
      // Translates to "expr(colA_minValue) < B" for index lookup
      case LessThan(sourceExpr @ SingleAttributeExpression(attrRef), value: Expression) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThan(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      case GreaterThan(value: Expression, sourceExpr @ SingleAttributeExpression(attrRef)) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThan(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      // Filter "B < expr(colA)" and "expr(colA) > B"
      // Translates to "B < colA_maxValue" for index lookup
      case LessThan(value: Expression, sourceExpr @ SingleAttributeExpression(attrRef)) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThan(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      case GreaterThan(sourceExpr @ SingleAttributeExpression(attrRef), value: Expression) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThan(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      // Filter "expr(colA) <= B" and "B >= expr(colA)"
      // Translates to "colA_minValue <= B" for index lookup
      case LessThanOrEqual(sourceExpr @ SingleAttributeExpression(attrRef), value: Expression) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThanOrEqual(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      case GreaterThanOrEqual(value: Expression, sourceExpr @ SingleAttributeExpression(attrRef)) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThanOrEqual(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      // Filter "B <= expr(colA)" and "expr(colA) >= B"
      // Translates to "B <= colA_maxValue" for index lookup
      case LessThanOrEqual(value: Expression, sourceExpr @ SingleAttributeExpression(attrRef)) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThanOrEqual(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      case GreaterThanOrEqual(sourceExpr @ SingleAttributeExpression(attrRef), value: Expression) if isSimpleExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThanOrEqual(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      // Filter "colA is null"
      // Translates to "colA_num_nulls > 0" for index lookup
      case IsNull(attribute: AttributeReference) =>
        getTargetIndexedColumnName(attribute, indexSchema)
          .map(colName => GreaterThan(genColNumNullsExpr(colName), Literal(0)))

      // Filter "colA is not null"
      // Translates to "colA_num_nulls = 0" for index lookup
      case IsNotNull(attribute: AttributeReference) =>
        getTargetIndexedColumnName(attribute, indexSchema)
          .map(colName => EqualTo(genColNumNullsExpr(colName), Literal(0)))

      // Filter "expr(colA) in (B1, B2, ...)"
      // Translates to "(colA_minValue <= B1 AND colA_maxValue >= B1) OR (colA_minValue <= B2 AND colA_maxValue >= B2) ... "
      // for index lookup
      // NOTE: This is equivalent to "colA = B1 OR colA = B2 OR ..."
      case In(sourceExpr @ SingleAttributeExpression(attrRef), list: Seq[Expression]) if list.forall(isSimpleExpression) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            list.map(lit => genColumnValuesEqualToExpression(colName, lit, targetExprBuilder)).reduce(Or)
          }

      // Filter "expr(colA) not in (B1, B2, ...)"
      // Translates to "NOT((colA_minValue = B1 AND colA_maxValue = B1) OR (colA_minValue = B2 AND colA_maxValue = B2))" for index lookup
      // NOTE: This is NOT an inversion of `in (B1, B2, ...)` expr, this is equivalent to "colA != B1 AND colA != B2 AND ..."
      case Not(In(sourceExpr @ SingleAttributeExpression(attrRef), list: Seq[Expression])) if list.forall(_.foldable) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            Not(list.map(lit => genColumnOnlyValuesEqualToExpression(colName, lit, targetExprBuilder)).reduce(Or))
          }

      // Filter "colA like 'xxx%'"
      // Translates to "colA_minValue <= xxx AND xxx <= colA_maxValue" for index lookup
      //
      // NOTE: Since a) this operator matches strings by prefix and b) given that this column is going to be ordered
      //       lexicographically, we essentially need to check that provided literal falls w/in min/max bounds of the
      //       given column
      case StartsWith(sourceExpr @ SingleAttributeExpression(attrRef), v @ Literal(_: UTF8String, _)) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            genColumnValuesEqualToExpression(colName, v, targetExprBuilder)
          }

      // Filter "expr(colA) not like 'xxx%'"
      // Translates to "NOT(expr(colA_minValue) like 'xxx%' AND expr(colA_maxValue) like 'xxx%')" for index lookup
      // NOTE: This is NOT an inversion of "colA like xxx"
      case Not(StartsWith(sourceExpr @ SingleAttributeExpression(attrRef), value @ Literal(_: UTF8String, _))) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            val minValueExpr = targetExprBuilder.apply(genColMinValueExpr(colName))
            val maxValueExpr = targetExprBuilder.apply(genColMaxValueExpr(colName))
            Not(And(StartsWith(minValueExpr, value), StartsWith(maxValueExpr, value)))
          }

      case or: Or =>
        val resLeft = createColumnStatsIndexFilterExprInternal(or.left, indexSchema)
        val resRight = createColumnStatsIndexFilterExprInternal(or.right, indexSchema)

        Option(Or(resLeft, resRight))

      case and: And =>
        val resLeft = createColumnStatsIndexFilterExprInternal(and.left, indexSchema)
        val resRight = createColumnStatsIndexFilterExprInternal(and.right, indexSchema)

        Option(And(resLeft, resRight))

      //
      // Pushing Logical NOT inside the AND/OR expressions
      // NOTE: This is required to make sure we're properly handling negations in
      //       cases like {@code NOT(colA = 0)}, {@code NOT(colA in (a, b, ...)}
      //

      case Not(And(left: Expression, right: Expression)) =>
        Option(createColumnStatsIndexFilterExprInternal(Or(Not(left), Not(right)), indexSchema))

      case Not(Or(left: Expression, right: Expression)) =>
        Option(createColumnStatsIndexFilterExprInternal(And(Not(left), Not(right)), indexSchema))

      case _: Expression => None
    }
  }

  private def checkColIsIndexed(colName: String, indexSchema: StructType): Boolean = {
    Set.apply(
      getMinColumnNameFor(colName),
      getMaxColumnNameFor(colName),
      getNumNullsColumnNameFor(colName)
    )
      .forall(stat => indexSchema.exists(_.name == stat))
  }

  private def getTargetIndexedColumnName(resolvedExpr: AttributeReference, indexSchema: StructType): Option[String] = {
    val colName = UnresolvedAttribute(getTargetColNameParts(resolvedExpr)).name

    // Verify that the column is indexed
    if (checkColIsIndexed(colName, indexSchema)) {
      Option.apply(colName)
    } else {
      None
    }
  }


  private def getTargetColNameParts(resolvedTargetCol: Expression): Seq[String] = {
    resolvedTargetCol match {
      case attr: Attribute => Seq(attr.name)
      case Alias(c, _) => getTargetColNameParts(c)
      case GetStructField(c, _, Some(name)) => getTargetColNameParts(c) :+ name
      case ex: ExtractValue =>
        throw new AnalysisException(s"convert reference to name failed, Updating nested fields is only supported for StructType: ${ex}.")
      case other =>
        throw new AnalysisException(s"convert reference to name failed,  Found unsupported expression ${other}")
    }
  }
}

private object ColumnStatsExpressionUtils {

  def genColMinValueExpr(colName: String): Expression =
    col(getMinColumnNameFor(colName)).expr
  def genColMaxValueExpr(colName: String): Expression =
    col(getMaxColumnNameFor(colName)).expr
  def genColNumNullsExpr(colName: String): Expression =
    col(getNumNullsColumnNameFor(colName)).expr

  /**
   * This check is used to validate that the expression that target column is compared against
   * <pre>
   *    a) Has no references to other attributes (for ex, columns)
   *    b) Does not contain sub-queries
   * </pre>
   *
   * This in turn allows us to be certain that Spark will be able to evaluate such expression
   * against Column Stats Index as well
   */
  def isSimpleExpression(expr: Expression): Boolean =
    expr.references.isEmpty && !SubqueryExpression.hasSubquery(expr)

  /**
   * This utility pattern-matches an expression iff
   *
   * <ol>
   *   <li>It references *exactly* 1 attribute (column)</li>
   *   <li>It does NOT contain sub-queries</li>
   * </ol>
   *
   * Returns only [[AttributeReference]] contained as a sub-expression
   */
  object SingleAttributeExpression {
    def unapply(expr: Expression): Option[AttributeReference] =
      if (SubqueryExpression.hasSubquery(expr) || expr.references.size != 1) {
        None
      } else {
        expr.references.head match {
          case attrRef: AttributeReference => Some(attrRef)
          case _ => None
        }
      }
  }

  def swapAttributeRefInExpr(sourceExpr: Expression, from: AttributeReference, to: Expression): Expression = {
    sourceExpr.transformDownWithPruning(_.containsAnyPattern(ATTRIBUTE_REFERENCE)) {
      case attrRef: AttributeReference if attrRef.sameRef(from) => to
    }
  }

  def genColumnValuesEqualToExpression(colName: String,
                                       value: Expression,
                                       targetExprBuilder: Function[Expression, Expression] = Predef.identity): Expression = {
    // TODO clean up
    checkState(isSimpleExpression(value))

    val minValueExpr = targetExprBuilder.apply(genColMinValueExpr(colName))
    val maxValueExpr = targetExprBuilder.apply(genColMaxValueExpr(colName))
    // Only case when column C contains value V is when min(C) <= V <= max(c)
    And(LessThanOrEqual(minValueExpr, value), GreaterThanOrEqual(maxValueExpr, value))
  }

  def genColumnOnlyValuesEqualToExpression(colName: String,
                                           value: Expression,
                                           targetExprBuilder: Function[Expression, Expression] = Predef.identity): Expression = {
    // TODO clean up
    checkState(isSimpleExpression(value))

    val minValueExpr = targetExprBuilder.apply(genColMinValueExpr(colName))
    val maxValueExpr = targetExprBuilder.apply(genColMaxValueExpr(colName))
    // Only case when column C contains _only_ value V is when min(C) = V AND max(c) = V
    And(EqualTo(minValueExpr, value), EqualTo(maxValueExpr, value))
  }
}

