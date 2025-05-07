import org.apache.spark.sql.SparkSession

/**
 * Step 5: Example Usage
 */
object SemFilterExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SemFilterPluginExample")
      .master("local[*]")
      .config("spark.jars", "/localhdd/hza214/spark-semantic-plugin/target/scala-2.12/spark-semantic-plugin_2.12-0.1.0-SNAPSHOT.jar")
      .config("spark.sql.extensions", "SemOperatorPlugin.SemSparkSessionExtension")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .config("spark.default.parallelism", "1")
      .getOrCreate()
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

    println(filteredDF.queryExecution.executedPlan)

    println("\nExecuting query and showing results...")
    filteredDF.show()

    println("\nStopping SparkSession.")
    spark.stop()
    println("SemFilter Example finished.")
  }
}