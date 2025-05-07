from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import your custom extension module
import SemanticExtension


spark = SparkSession.builder.appName("SemFilterExample") \
      .master("local[*]") \
      .config("spark.jars", "/localhdd/hza214/spark-semantic-plugin/target/scala-2.12/spark-semantic-plugin_2.12-0.1.0-SNAPSHOT.jar") \
      .config("spark.sql.extensions", "SemOperatorPlugin.SemSparkSessionExtension") \
      .config("spark.driver.extraClassPath", "/localhdd/hza214/spark-semantic-plugin/target/scala-2.12/spark-semantic-plugin_2.12-0.1.0-SNAPSHOT.jar") \
      .config("spark.executor.extraClassPath", "/localhdd/hza214/spark-semantic-plugin/target/scala-2.12/spark-semantic-plugin_2.12-0.1.0-SNAPSHOT.jar") \
      .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
      .config("spark.default.parallelism", "1") \
      .getOrCreate() \

SemanticExtension.add_sem_filter_to_dataframe()

# Create a sample DataFrame
data = [("Alice", 25, "HR"), ("Bob", 30, "Engineering"), ("Charlie", 22, "HR")]
columns = ["name", "age", "department"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

try:
    filtered_df_str = df.sem_filter("{age} is larger than 23")
    filtered_df_str.show()
except Exception as e:
    print(f"An error occurred while using sem_filter with a string: {e}")
    print("Ensure your Java/Scala 'sem_filter(String)' implementation is correct and available.")

spark.stop()