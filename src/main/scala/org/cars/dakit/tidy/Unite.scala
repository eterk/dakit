package org.cars.dakit.tidy

import org.apache.spark.sql.functions.{concat, struct}
import org.apache.spark.sql.{Column, DataFrame}

import scala.annotation.tailrec


trait Unite {
  val func: collection.Seq[Column] => Column

  private def unite_col(cols: Seq[Column]): Column = func(cols)

  def structWarp(seq: Seq[Column]): Column = struct(seq: _*)


  def concatWarp(seq: Seq[Column]): Column = concat(seq: _*)

  def unite(df: DataFrame, newCol: String, cols: Seq[Column]): DataFrame = {
    df.withColumn(newCol, unite_col(cols))
  }

  @tailrec
  final def unites(df: DataFrame, newCols: Seq[String], cols: Seq[Seq[Column]]): DataFrame = {
    if (newCols.isEmpty) return df
    unites(unite(df, newCols.head, cols.head), newCols.tail, cols.tail)
  }

}

object Unite {

  case class StructUnite() extends Unite {
    override val func: Seq[Column] => Column = structWarp
  }

  case class ConcatUnite() extends Unite {
    override val func: Seq[Column] => Column = concatWarp
  }

  def insertSep(originCol: Seq[Column], sep: Column): Seq[Column] = {

    val result = collection.mutable.ArrayBuffer[Column]()
    var first = true
    for (x <- originCol) {
      if (first) {
        result.append(x)
        first = false
      }
      else {
        result.append(sep)
        result.append(x)
      }
    }
    result
  }
}
