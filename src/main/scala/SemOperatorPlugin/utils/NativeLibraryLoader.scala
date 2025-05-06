package SemOperatorPlugin.utils

object NativeLibraryLoader {

  def load(): Unit = {
    _load
  }

  private lazy val _load: Boolean = {
      println("Loading native library...")
      System.load("/localhdd/hza214/spark-semantic-plugin/semantic-native/target/release/libsemantic_native.so") // Replace with the actual path to your .so file
      // TODO change to loadLibrary(xxx.so)
      println("Library loaded")
    true
  }
}