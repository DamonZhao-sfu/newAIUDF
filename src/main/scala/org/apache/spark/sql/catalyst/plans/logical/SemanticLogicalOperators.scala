package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * Step 1: Define the SemFilter logical operator.
 * It extends UnaryNode, taking a parsed Expression and a child plan.
 */

case class SemFilterLogical(prompt: String, child: LogicalPlan)
  extends UnaryNode with PredicateHelper {
  override def output: Seq[Attribute] = child.output

  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition
  override def nodeName: String = "SemFilter"
  final override val nodePatterns: Seq[TreePattern] = Seq(TreePattern.FILTER)

  override protected def withNewChildInternal(newChild: LogicalPlan): SemFilterLogical =
    copy(child = newChild)
}