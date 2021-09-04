package org.cars.dakit

import org.apache.spark.sql.{Dataset, SqlVi}
import org.apache.spark.sql.types.{StructField, StructType}
import org.cars.dakit.sql.DataFrameBasic
import org.cars.dakit.test.TestHolder
import org.scalatest.FunSuite


case class R1(
               r11: R11,
               r12: R12
             )

case class R11(
                r111: Int,
                r112: Boolean,
                r113: Seq[String]
              )

case class R12(
                r121: Map[Float, Double],
                r122: Long,
                r123: String
              )

case class RenameFor(
                      r1: R1,
                      r2: String
                    )

class DataFrameBasicTest extends FunSuite with TestHolder {

  import spark.implicits._

  val data = spark.emptyDataset[RenameFor]


  //  lookup.foldLeft(df)((acc, ca) => acc.withColumnRenamed(ca._1, ca._2))
  def renameStruct(df: Dataset[_], existsName: String, newName: String): Unit = {
    require(newName.split("\\.").size == 1)
    val nest = existsName.split("\\.")
    if (nest.size == 1) {

    }
    val newSchema = df.schema(nest.head)
  }

  def creatNew(con: Seq[StructType], instead: StructField): StructType = {

    null

  }



  def structs(nestNames: Seq[String], newName: String, con: Seq[StructType]): StructType = {
    if (nestNames.size == 1) {
      val sf = con.last(nestNames.head)
      val newSf = StructField(newName, sf.dataType, sf.nullable, sf.metadata)
      val conRe = con.reverse

      creatNew(conRe, newSf)
    }

    null
  }


}
