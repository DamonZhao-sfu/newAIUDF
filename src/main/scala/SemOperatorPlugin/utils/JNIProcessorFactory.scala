package SemOperatorPlugin.utils

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema

object JNIProcessorFactory {
  NativeLibraryLoader.load()


  private class Initializer {
    @native def initSemanticFilterProcessor(prompt: String): Long
  }

  def semanticFilterProcessor(prompt: String, outputSchema: Schema, allocator: BufferAllocator): JNIProcessor = {
    //val ptr = jni.initSemanticFilterProcessor(prompt)
    new JNIProcessor(prompt, outputSchema)
  }
}