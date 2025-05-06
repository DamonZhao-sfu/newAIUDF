package SemOperatorPlugin.utils

import org.apache.arrow.vector.{FieldVector, VectorLoader, VectorSchemaRoot}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

object ArrowColumnarConverters {

  implicit class ColumnarBatchUnloader(columnarBatch: ColumnarBatch){
    def toArrow: VectorSchemaRoot = {
      val columnVectors = (0 until columnarBatch.numCols).map(columnarBatch.column)
      columnVectors.foreach { vector =>
        println(s"Column type: ${vector.getClass.getName}")
      }

      val matches = columnVectors.forall(_.isInstanceOf[ArrowColumnVector])
      if (!matches) throw new IllegalArgumentException("ColumnarBatch cannot be loaded into a VectorSchemaRoot.")

      val fieldVectors = columnVectors.map(_.asInstanceOf[ArrowColumnVector]).map(extractVector).asJava
      new VectorSchemaRoot(fieldVectors)
    }
  }

  implicit class VectorSchemaRootUnloader(root: VectorSchemaRoot) {
    lazy val loader = new VectorLoader(root)
    def toBatch: ColumnarBatch = {
      val columnVectors: Array[ColumnVector] = root
        .getFieldVectors
        .asScala
        .map(new ArrowColumnVector(_))
        .toArray
      new ColumnarBatch(columnVectors, root.getRowCount)
    }
  }

  private lazy val accessorField = {
    val accessorField = classOf[ArrowColumnVector].getDeclaredField("accessor")
    accessorField.setAccessible(true)
    accessorField
  }

  private lazy val vectorField = {
    val accessorClass: Class[_] = classOf[ArrowColumnVector].getDeclaredClasses.find(_.getSimpleName == "ArrowVectorAccessor")
      .getOrElse(throw new Exception("Cannot access class 'org.apache.spark.sql.vectorized.ArrowColumnVector.ArrowVectorAccessor' by reflection."))
    val vectorField = accessorClass.getDeclaredField("vector");
    vectorField.setAccessible(true)
    vectorField
  }

  private def extractVector(arrowColumnVector: ArrowColumnVector): FieldVector = {
    val accessor = accessorField.get(arrowColumnVector)
    vectorField.get(accessor).asInstanceOf[FieldVector]
  }
}
