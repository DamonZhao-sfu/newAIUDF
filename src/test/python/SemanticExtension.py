# pyspark_sem_filter_extension.py

from pyspark.sql import DataFrame
from pyspark.sql.column import Column
# For type hinting, ColumnOrName is often Union[Column, str]
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    # This helps with type hinting for DataFrame and SparkSession
    # without causing circular imports if this module is imported early.
    from pyspark.sql import SparkSession

def sem_filter_impl(self: DataFrame, prompt: str) -> DataFrame:
    if not hasattr(self, 'sparkSession') or self.sparkSession is None:
        raise RuntimeError(
            "SparkSession not available on DataFrame. "
            "This usually means the DataFrame was not created correctly or "
            "the extension is being used in an unsupported way."
        )

    if isinstance(prompt, str):
        # Call the Java sem_filter method with a String argument
        try:
            jdf = self._jdf.semFilter(prompt)
        except Exception as e:
            print(f"ERROR: Call to Java _jdf.semFilter('{prompt}') failed.")
            print("Ensure 'semFilter(String)' is defined on your Spark's Java/Scala DataFrame (Dataset) class.")
            raise e
    else:
        raise TypeError(
            f"Condition for sem_filter must be a PySpark Column or a SQL string expression. "
            f"Got type: {type(prompt)}"
        )
    return DataFrame(jdf, self.sparkSession)

def add_sem_filter_to_dataframe():
    if not hasattr(DataFrame, 'sem_filter'):
        DataFrame.sem_filter = sem_filter_impl
        print("SUCCESS: 'sem_filter' method has been added to pyspark.sql.DataFrame.")
    else:
        print("INFO: 'sem_filter' method already exists on pyspark.sql.DataFrame. Patch not applied again.")
