package org.cars.dakit.sql

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.reflect.{ClassTag, classTag}

object DataTypeTool {
  /** 检查 data 是否存在 指定字段 colName ,colName 是否为 T 类
   * 注意在 ArrayType 中 只可以检查到 ArrayType 层面，如果具体到Array 内类型 可以使用
   * {{{
   *    def colCheck=classTag[T].runtimeClass.isInstance(Option(data.schema(colName)).get.dataType)
   *    c2(data, "array_int", ArrayType(IntegerType, false))
   * }}}
   *
   */
  def colCheck[T: ClassTag](data: Dataset[_], colName: String): Boolean = {
    try {
      classTag[T].runtimeClass.isInstance(Option(data.schema(colName)).get.dataType)
    } catch {
      case _: java.lang.IllegalArgumentException => false
    }
  }

  /** 获取指定类型的列名
   */
  def specifyTypeColGet[T: ClassTag](data: DataFrame): Seq[String] = {
    data.columns.filter(colCheck[T](data, _)).toSeq
  }


  /**
   * 比较两个 structType
   */
  private[dakit] def getDiffDataType(current: StructType, target: StructType): Map[String, (DataType, DataType)] = {

    require(current.names.mkString(",").equals(target.names.mkString(",")), "names must same size and order")
    target.
      zip(current).
      map(s => (s._1.name, (s._1.dataType, s._2.dataType))).toMap.
      filter(p => p._2._1 != p._2._2)
  }


}
