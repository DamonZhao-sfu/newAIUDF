package SemOperatorPlugin

import org.apache.spark.sql.{Dataset, Encoder, SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SemanticFilterExec
import org.apache.spark.sql.catalyst.plans.logical.SemFilterLogical
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.reflect.runtime.universe.TypeTag

class SemSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  // Define the strategy as an inner object or companion object


  object SemanticOperatorsStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Match the logical node
      case logical.SemFilterLogical(prompt, child) =>
        // Plan the child node first using planLater (recursive planning)
        // Create the physical node
        SemanticFilterExec(prompt, planLater(child)) :: Nil
      case _ =>
        Nil
    }
  }

  override def apply(extensions: SparkSessionExtensions): Unit = {
    println("Registering SemFilterSparkSessionExtension...")
    extensions.injectPlannerStrategy(_ => SemanticOperatorsStrategy)
  }
}