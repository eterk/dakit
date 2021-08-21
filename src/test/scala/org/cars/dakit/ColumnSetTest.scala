package org.cars.dakit

import java.sql.Timestamp
import java.time.LocalDate

import org.apache.spark.sql.functions.{col, to_date}
import org.cars.dakit.sql.ColumnSet._
import org.cars.dakit.basic.TimestampTool
import org.cars.dakit.test.TestHolder

import scala.util.Random

class ColumnSetTest extends org.scalatest.FunSuite with TestHolder {


  TimestampTool

  test("t1") {

    val manyInt = (nums: Int, max: Int) => (0 until nums).map(x => new Random(x + 1453).nextInt(max))

    val infox: Seq[(Timestamp, String, Int)] = TimestampTool.manyRandomTimestamp(50).
      zip(TimestampTool.manyRandomHourMinute(50)).zip(manyInt(50, 50)).map(x => (x._1._1, x._1._2, x._2))

    val x1 = spark.
      createDataFrame(infox).
      toDF("time", "hour_minute", "int_days").
      withColumn("date", to_date(col("time"))).
      withColumn("date8", timestamp_to_date8(col("time"))).
      withColumn("date1", date8_to_timestamp(col("date8"))).
      withColumn("min_time", hour_min_to_min_of_day(col("hour_minute"))).
      withColumn("hour_minute1", min_of_day_to_hour_min(col("min_time"))).
      withColumn("time_add", timestamp_add_days(col("time"), col("int_days"))).
      withColumn("date1_add", timestamp_add_days(col("date1"), -col("int_days")))

  }
  test("dateRange") {

    DF3.filter(inRange(col("time"), LocalDate.of(2014, 1, 1) -> LocalDate.of(2016, 1, 1))).show()

  }

  test("dasdsa") {
    import spark.implicits._

    val df = Seq(("as", "s", "ba"), ("dsa", "d", "c"), ("12", "ds", "xs")).
      toDF("a", "b", "c")
    import org.apache.spark.sql.functions.lit
    val df2 = df.withColumn("sep", sort_str_concat(lit("_"), col("a"), col("b"), col("c")))
    df2.show()


  }


}
