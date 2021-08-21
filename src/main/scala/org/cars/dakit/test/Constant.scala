package org.cars.dakit.test

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.cars.dakit.basic.TimestampTool


object Constant {

  private val spark = SparkSession.active

  val emptyIntArray = array().cast(ArrayType(IntegerType))

  val DATE8: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  val DATE10: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  /**
   * 在测试中用到的代码
   */
  case class TestCase(array_int: Array[Int])

  /**
   * 在测试中用到的代码
   */
  def simpleData: DataFrame = {

    val df_list = List(("", 1, Seq(1, 2, 3), 2L, 3.1))
  // java.lang.UnsupportedOperationException: `long` is a reserved keyword and cannot be used as field name
    spark.createDataFrame(df_list).
      toDF("string", "int", "array_int", "long", "double").
      withColumn("struct_int_long", struct(col("int"), col("long"))).
      withColumn("date", to_date(lit("2020-02-02"))).
      withColumn("time_stamp", to_timestamp(lit("2020-02-20 02:02:02")))
  }

  def someData: DataFrame = {
    spark.createDataFrame(Seq(("das", "dasdas", "dasdasd", 1, 1L, 1.2, LocalDate.of(2020, 1, 1).toString),
      ("das", "dasdas", "dasdasd", 1, 1L, 1.2, LocalDate.of(2020, 1, 1).toString))).
      withColumn("date", to_date(col("_7"))).
      withColumn("datetime", to_timestamp(col("date")))
  }


  /**
   * 返回随机timestamp的dataframe
   *
   * @param nums row 长度
   * @param name row 名称
   */
  def randomTimesTampDf(nums: Int, name: String): DataFrame = {
    spark.createDataFrame(TimestampTool.manyRandomTimestamp(nums).zipWithIndex).toDF(name, "index").drop("index")
  }


}
