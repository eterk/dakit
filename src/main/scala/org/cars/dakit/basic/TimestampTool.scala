package org.cars.dakit.basic

import java.sql.Timestamp

object TimestampTool {


  /**
   * 获取某个时间范围内的 一个timestamp
   *
   * @param start
   * @param diff
   * @return
   */
  def randomTimestamp(start: Long, diff: Long): Timestamp = {
    new Timestamp(start + (Math.random() * diff).toLong)
  }

  /**
   * 随机获取一些timestamp
   */
  def manyRandomTimestamp(nums: Int): Seq[Timestamp] = {
    val (start, end) = (Timestamp.valueOf("2017-01-01 00:00:00").getTime, Timestamp.valueOf("2025-12-31 00:00:00").getTime)
    val diff = start - end + 1
    for (_ <- (1 to nums)) yield randomTimestamp(start, diff)
  }

  def randomHourMinute: String = {
    val hour = (Math.random() * 24).toInt.toString
    val minute = (Math.random() * 60).toInt.toString

    val pad = (x: String) =>
      if (x.length == 1) {
        "0" + x
      } else {
        x
      }
    pad(hour).toString + pad(minute).toString
  }

  def manyRandomHourMinute(nums: Int): Seq[String] = {
    for (_ <- (1 to nums)) yield randomHourMinute
  }


}
