package SemOperatorPlugin.utils

import org.apache.arrow.memory.BufferAllocator

import java.util.function.UnaryOperator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.{VectorLoader, VectorUnloader}

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import scala.collection.JavaConverters._
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.c.ArrowArrayStream
import org.apache.spark.TaskContext
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import scala.util.matching.Regex

class JNIProcessor(prompt: String, outputSchema: Schema) extends UnaryOperator[VectorSchemaRoot] with ClosableFunction[VectorSchemaRoot,VectorSchemaRoot] {

  NativeLibraryLoader.load()


  private lazy val allocator = GlobalAllocator.newChildAllocator(this.getClass)

  private lazy val rootOut = VectorSchemaRoot.create(outputSchema, allocator)

  @native private def process(
                                prompt: String,
                                inputArrayPtr: Long,
                                inputSchemaPtr: Long,
                                outputArrayPtr: Long,
                                outputSchemaPtr: Long
                              ): Long

  // Declare the native method for closing the Rust-side processor
  @native private def close(ptr: Long): Unit // Renamed to avoid conflict with AutoCloseable.close

  private def extractColumnNames(prompt: String): List[String] = {
    val pattern: Regex = """\{([^}]+)\}""".r
    val matches = pattern.findAllIn(prompt).matchData.map(_.group(1)).toList
    if (matches.isEmpty) {
      throw new IllegalArgumentException(s"No column names found in prompt: $prompt")
    }
    matches
  }

  override def apply(root: VectorSchemaRoot): VectorSchemaRoot = {
    val inputArray = ArrowArray.allocateNew(allocator)
    val inputSchema = ArrowSchema.allocateNew(allocator)

    // Export the VectorSchemaRoot to C Data interface
    Data.exportVectorSchemaRoot(allocator, root, new CDataDictionaryProvider(),
      inputArray, inputSchema)

    // Prepare output C Data interfaces
    val outputArrayBuf = ArrowArray.allocateNew(allocator)
    val outputSchemaBuf = ArrowSchema.allocateNew(allocator)

    val columnNames = extractColumnNames(prompt)
    try {
      // Call the native method
      process(
        prompt,
        inputArray.memoryAddress(),
        inputSchema.memoryAddress(),
        outputArrayBuf.memoryAddress(),
        outputSchemaBuf.memoryAddress()
      )
      Data.importVectorSchemaRoot(allocator, outputArrayBuf, outputSchemaBuf, new CDataDictionaryProvider())
    } catch {
      case e: Exception =>
        // Clean up resources in case of error
        inputArray.close()
        inputSchema.close()
        outputArrayBuf.close()
        outputSchemaBuf.close()
        throw e
    }

  }

  private case class BufferDescriptor(root: VectorSchemaRoot) {
    def close(): Any = recordBatch.close();

    lazy val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch
    //lazy val rowCount: Int = root.getRowCount
    lazy val addresses: Array[Long] = recordBatch.getBuffers.asScala.map(_.memoryAddress()).toArray
    lazy val sizes: Array[Long] = recordBatch.getBuffersLayout.asScala.map(_.getSize()).toArray

  }

  override def close(): Unit = {
    rootOut.close()
  };

}