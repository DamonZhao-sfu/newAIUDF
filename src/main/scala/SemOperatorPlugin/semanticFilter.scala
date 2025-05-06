package SemOperatorPlugin

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SemanticFilterExec

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


/**
 * Step 5: Example Usage
 */
object SemFilterExample {
  def main(args: Array[String]): Unit = {
    println("Starting SemFilter Example...")

    val spark = SparkSession.builder()
      .appName("SemFilterPluginExample")
      .master("local[*]")
      .config("spark.jars", "/localhdd/hza214/spark-semantic-plugin/target/scala-2.12/spark-semantic-plugin_2.12-0.1.0-SNAPSHOT.jar")
      .config("spark.sql.extensions", "SemOperatorPlugin.SemSparkSessionExtension")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .config("spark.default.parallelism", "1")
      .getOrCreate()

    println("SparkSession created with SemFilter extension.")

    // Import implicits for .toDF() and the SemFilter extension method
    import spark.implicits._

    // Create a sample DataFrame
    val data = Seq(("Alice", 25), ("Bob", 30), ("Cathy", 28), ("David", 22))
    val df = data.toDF("name", "age")
    println("Original DataFrame:")
    df.show()

    // Apply SemFilter using the extension method
    val filteredDF = df.semFilter(" {name} equals to Alice").semFilter("{age} > 25") // Use a slightly more complex filter

    println("\nLogical Plan after Optimization (contains SemFilter):")
    println(filteredDF.queryExecution.logical)
    println("\nPhysical Plan after Optimization (contains SemFilter):")
    // Use explain(extended = false) for a cleaner logical plan view
    // Note: You might see 'UnresolvedRelation' initially if optimizations haven't run
    println(filteredDF.queryExecution.executedPlan)

    println("\nExecuting query and showing results...")
    // When an action like show() is called, Spark analyzes, optimizes, and executes the plan.
    // Our optimizer rule will run during the optimization phase.
    filteredDF.show()

    println("\nStopping SparkSession.")
    Thread.sleep(1000000) // Sleep for 1 second

    spark.stop()
    println("SemFilter Example finished.")
  }
}


