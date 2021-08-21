package org.cars.dakit.test

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/** *
 * common 包测试使用的简单spark 实例
 */
object SparkFactory {
  //  todo 本机测试的hive 支持
  def localSpark =
    if (SparkEnv.get == null || SparkEnv.get.executorId.equalsIgnoreCase("driver")) {
      SparkSession
        .builder()
        .enableHiveSupport()
        .master("local[4]")
//        .config("hive.exec.scratchdir", "../local_data")
        .appName("TestSpark")
        .getOrCreate()
    } else null

  lazy val spark = {
    Try(SparkSession.active) match {
      case Success(v) => v
      case Failure(e) => localSpark
    }
  }
  spark.sparkContext.setLogLevel("warn")


}
