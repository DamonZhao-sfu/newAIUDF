package org.apache.spark.sql.execution

import SemOperatorPlugin.utils.{ArrowColumnarConverters, ArrowWriter, ClosableFunction, GlobalAllocator, JNIProcessor, JNIProcessorFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.vectorized.ColumnarArray
import SemOperatorPlugin.utils.ArrowColumnarConverters._



case class SemanticFilterExec(prompt: String, child: SparkPlan) extends UnaryExecNode {
  // Split out all the IsNotNulls from condition.
  override def supportsColumnar: Boolean = true

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.

  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  private def columnarBatchToSubsetVectorSchemaRoot(
                                                     batch: ColumnarBatch,
                                                     newSchema: Schema): VectorSchemaRoot = {
    val allocator = GlobalAllocator.newChildAllocator(this.getClass)
    try {

      // Create new VectorSchemaRoot with selected fields
      val newRoot = VectorSchemaRoot.create(newSchema, allocator)
      val arrowWriter = ArrowWriter.create(newRoot)

      // Write columnar batch data directly to new root
      val rowsToProduce = batch.numRows()
      for (columnIndex <- 0 until batch.numCols()) {
        val column = batch.column(columnIndex)
        val columnArray = new ColumnarArray(column, 0, rowsToProduce)
        if (column.hasNull) {
          arrowWriter.writeCol(columnArray, columnIndex)
        } else {
          arrowWriter.writeColNoNull(columnArray, columnIndex)
        }
      }
      arrowWriter.finish()
      newRoot.setRowCount(batch.numRows())
      newRoot
    } catch {
      case e: Throwable =>
        allocator.close()
        throw e
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Extract column names once
    // Get timezone ID for schema conversion
    val timeZoneId = conf.sessionLocalTimeZone
    // Use mapPartitions for per-partition setup and cleanup
    child.executeColumnar().mapPartitions { batchIterator =>
      // Get partition's allocator (reuse if possible, or create new per partition)
      // Consider using ArrowUtils.rootAllocator for simplicity if suitable
      val arrowSchema = ArrowUtils.toArrowSchema(child.schema, timeZoneId) // Convert Spark schema to Arrow schema

      // For filter processor, the return dataframe schema should be the same as the input one
      val jniProcessor = new JNIProcessor(prompt, arrowSchema)
      // Ensure processor is closed when the task completes (success or failure)
      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        jniProcessor.close()
      }

      // Return an iterator that processes batches and handles resources
      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = batchIterator.hasNext
        override def next(): ColumnarBatch = {
          val inputBatch = batchIterator.next()
          var processedRoot: VectorSchemaRoot = null
          var outputBatch: ColumnarBatch = null
          try {
            var arrowVector = columnarBatchToSubsetVectorSchemaRoot(inputBatch, arrowSchema)
            processedRoot = jniProcessor.apply(arrowVector)
            outputBatch = processedRoot.toBatch
            arrowVector = null
            processedRoot = null
            outputBatch

          } finally {
            if (processedRoot != null) processedRoot.close()
          }
        } // End next()
      } // End iterator
    } // End mapPartitions
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SemanticFilterExec =
    copy(child = newChild)
}