package org.cars.dakit.test

import java.sql.Timestamp

import org.apache.spark.sql.Dataset


private[dakit] trait TestHolder extends TestUtil {

  val LOCALDIR = "test_dir/"

  val spark = SparkFactory.spark

  case class DemoClass(depart_date: Timestamp,
                       name: String,
                       order: Int,
                       total: Double,
                       values: Seq[Double])


  lazy val DF1 = Constant.simpleData

  lazy val DF2 = Constant.someData
  lazy val DF3 = Constant.randomTimesTampDf(10, "time")


}

object TestHolder extends TestHolder
