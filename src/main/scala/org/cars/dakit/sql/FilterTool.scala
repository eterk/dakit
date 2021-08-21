package org.cars.dakit.sql

import java.time.LocalDate

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.cars.dakit.basic.BasicToolSet

import scala.annotation.tailrec

object FilterTool {
  def filterMapParse(key: String, dType: DataType, value: Object): Option[Column] = {
    value match {
      case seq: Seq[_] => seqParse(key, dType, seq)
      case range: (_, _) => tuple2Parse(key, dType, range)
      case eqStr: String => eqStrParse(key, dType, eqStr)
      case localDate: LocalDate=>eqStrParse(key, dType, localDate.toString)
      case caseClass: Product if dType.isInstanceOf[StructType] => caseClassParse(key, value)
    }
  }

  def caseClassParse[T](key: String, value: Object): Option[Column] = {
    val m = BasicToolSet.fieldsToMap(value)

    m.map(x => (col(key + "." + x._1), x._2.asInstanceOf[Object]))

    Option(lit(null))
    throw new IllegalArgumentException("尚未实现")
  }

  def seqParse(key: String, dType: DataType, value: Seq[_]): Option[Column] = {
    value match {
      case "*" :: Nil => None
      case _ => Option(col(key).isInCollection(value))
    }

  }

  def eqStrParse(key: String, dType: DataType, value: String): Option[Column] = {
    value match {
      case "*" => None
      case _ => Option(col(key) === value)
    }
  }

  def tuple2Parse(key: String, dType: DataType, value: (_, _)): Option[Column] = {
    value match {
      case (f: LocalDate, c: LocalDate) => Option(ColumnSet.inRange(col(key), (f, c)))
      case (f: Int, c: Int) => Option(col(key).between(f, c))
      //      case dateRange if dateRange._1.isInstanceOf[LocalDate] && dateRange._2.isInstanceOf[LocalDate] => Option(ColumnSet.inRange(col(key), (dateRange._1.asInstanceOf[LocalDate], dateRange._2.asInstanceOf[LocalDate])))
    }
  }


  private def check(dfCol: Seq[String], keyCol: Seq[String]) = {
    require(BasicToolSet.allIn(dfCol, keyCol), println("all col must in data " + keyCol.diff(dfCol).mkString(",")))
  }


  def filters(data: DataFrame, fMap: Map[String, Object], report: Boolean): DataFrame = {
    val maps = fMap.groupBy(x => x._2 match {
      case ds: Dataset[_] => 1
      case _ => 2
    })
    data.transform(df => {
      if (maps.keySet.contains(1)) {
        val map1 = maps(1).map(x => parserDF(x._1, x._2))
        semiFilters(data, map1, report)
      } else {
        df
      }
    }).transform(df => {
      if (maps.keySet.contains(2)) {
        filterColumns(df, maps(2), report)
      } else {
        df
      }
    })
  }

  def parserDF(key: String, value: Object): (Seq[String], Dataset[_]) = {

    (key.split(",").toSeq, value.asInstanceOf[Dataset[_]])
  }


  private def filterColumns(data: DataFrame, fMap: Map[String, Object], report: Boolean): DataFrame = {
    val keyCol = fMap.keySet.toSeq
    check(data.columns, keyCol)

    val filterCols = data.schema.
      filter(x => keyCol.contains(x.name)).
      map(x => (x.name, x.dataType, fMap(x.name))).
      map(x => filterMapParse(x._1, x._2, x._3)).
      filterNot(x => x.isEmpty).
      map(x => x.get)

    filters(data, filterCols, "", report)
  }


  private def semiFilters(data: DataFrame, fMap: Map[Seq[String], Dataset[_]], report: Boolean): DataFrame = {
    check(data.columns, fMap.keySet.flatten.toSeq)
    fMap.foreach(x => check(x._2.columns, x._1))

    semiFilters_(data, fMap, report)

  }

  @tailrec
  private final def semiFilters_(data: DataFrame, fMap: Map[Seq[String], Dataset[_]], report: Boolean): DataFrame = {
    if (fMap.isEmpty) return data

    val (joinCol, semiDf) = fMap.head

    val newData = data.join(semiDf, joinCol, "left_semi")


    if (report) {
      newData.cache()
      println("leftSemi with" + joinCol.mkString(",") + " " + newData.count())
    }

    semiFilters_(newData, fMap.tail, report)
  }


  @tailrec
  private final def filters(data: DataFrame, cols: Seq[Column], lastKey: String, report: Boolean): DataFrame = {

    if (!lastKey.isEmpty & report) {
      data.cache()
      println(lastKey + data.count())
    }

    if (cols.isEmpty) {
      data
    } else {
      val new_data = data.filter(cols.head)
      filters(new_data, cols.tail, lastKey + cols.head + ";", report)
    }
  }


}
