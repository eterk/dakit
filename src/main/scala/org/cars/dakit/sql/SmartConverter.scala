package org.cars.dakit.sql

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.cars.dakit.sql.DataFrameBasic.{logger, select, withColumns}
import org.cars.dakit.tidy.Tidy


object SmartConverter {

  import scala.reflect.runtime.universe.TypeTag


  def typeAnaly(data: Dataset[_], result: Dataset[_]): Dataset[_] = {

    val flatten = Tidy.separate(data, true, true) //faltten all of struct type
    val result_flatten = Tidy.separate(result, true, true)
    val (allIn, miss) = aInb[StructField](result_flatten.schema, flatten.schema, _.name == _.name)


    val strDistinct = (seq: Seq[String]) => seq.toSet.size == seq.size
    require(strDistinct(flatten.columns), "exists duplicate field,check please")
    require(strDistinct(result_flatten.columns), "exists duplicate field,check please")

    require(allIn, miss.map(x => s"${x.name} is miss"))

    val flatten_new = flatten.select(result_flatten.columns.head, result_flatten.columns.tail: _*)


    val diffType = DataTypeTool.getDiffDataType(flatten_new.schema, result_flatten.schema).
      filter(p => p._2._2.typeName != p._2._2.typeName)

    val after_convert = if (diffType.isEmpty) {
      flatten_new
    } else {
      val colMap = diffType.map(p => (p._1, col(p._1).cast(p._2._1)))
      diffType.foreach(f =>
        logger.warn("col " + f._1 + " expect " + f._2._1.simpleString + " get " + f._2._2.simpleString + "  cast automatic")
      )
      withColumns(flatten_new, colMap)
    }
    uniteAuto(result.schema, after_convert.toDF())
  }

  def uniteAuto(target: Seq[StructField], flattenDs: DataFrame): DataFrame = {
    uniteAuto_(target, flattenDs, "")
  }

  private def uniteAuto_(target: Seq[StructField], flattenDs: DataFrame, parent: String): DataFrame = {

    var ds = flattenDs
    val structs = target.filter(c => c.dataType.isInstanceOf[StructType])

    if (structs.nonEmpty) {
      println(parent)

      val ite = structs.iterator

      while (ite.hasNext) {
        val c = ite.next()
        println(c.name)
        val cStruct = c.dataType.asInstanceOf[StructType]
        ds = uniteAuto_(cStruct, ds, c.name)
      }
    }

    if (parent.isEmpty) {
      ds
    } else {
      Tidy.unite(ds, parent, target.map(_.name), true)
    }


  }


  def aInb[T](a: Seq[T], b: Seq[T], check: (T, T) => Boolean): (Boolean, Seq[T]) = {
    val result =
      a.map(x => {
        val exist =
          b.exists(y => {
            check(x, y)
          })

        (x, exist)
      }).filter(!_._2)
    (result.isEmpty, result.unzip._1)
  }

  def productConvert[T <: Product : TypeTag](data: Dataset[_]): Dataset[T] = {
    val encoders = Encoders.product[T]

    val r = SparkSession.active.emptyDataset[T](encoders)
    val result = typeAnaly(data, r)

    result.as[T](encoders)
  }

}
