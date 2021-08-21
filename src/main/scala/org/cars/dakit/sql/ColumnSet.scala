package org.cars.dakit.sql

import java.sql.Timestamp
import java.time.LocalDate
import java.util.Calendar

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.cars.dakit.DateRange
import org.cars.dakit.basic.{C, DateToolSet}

import scala.annotation.{tailrec, varargs}

/** *
 * 一些常用的 列字段
 * 这个api 类似与 `org.apache.spark.sql.functions`
 *
 * @author K
 *
 */
object ColumnSet {


  def sumDoubleColField(colName: String, field: String): Column = {
    expr(s"AGGREGATE($colName, CAST(0.0 AS double), (accumulator, item) -> accumulator + item.$field)")
  }

  def inRange(dateCol: Column, dateRange: DateRange): Column = {
    dateCol.between(to_timestamp(lit(dateRange._1.toString)), to_timestamp(lit(dateRange._2.toString)))
  }


  def dateRangeBetween(dateCol: Column, dateRange: DateRange): Column = {
    val t = DateToolSet.toTimestampRange(dateRange)
    dateCol >= to_timestamp(lit(t._1)) && (dateCol <= to_timestamp(lit(t._2)))
  }


  /**
   * localDate 转换为timestamp 格式的列
   *
   * @return
   */
  def local_date_to_timestamp(date: LocalDate): Column = {
    to_timestamp(lit(date.toString))
  }


  private val tsAddDays = (t: Timestamp, d: Int) => {
    val cal = Calendar.getInstance()
    cal.setTime(t)
    cal.add(Calendar.DATE, d)
    new Timestamp(cal.getTime().getTime)
  }
  private val timestamp_add_days_udf = udf(tsAddDays)

  /**
   * @param ts  timestamp 列
   * @param int 添加的日期长度
   */
  def timestamp_add_days(ts: Column, int: Column): Column = {
    timestamp_add_days_udf(ts, int)
  }
  /**
   * yyyyMMdd 转化为 timestamp 格式
   * {{{
   *   date8ToTimestamp(lit("20210101"))
   * }}}
   */
  def date8_to_timestamp(colIn: Column): Column = to_timestamp(colIn, "yyyyMMdd")

  /**
   * timestamp 转化为 yyyyMMdd 格式
   * {{{
   *   timestampToDate8(date8ToTimestamp(lit("20210101")))
   * }}}
   */
  def timestamp_to_date8(colIn: Column): Column = date_format(colIn, "yyyyMMdd")

  /**
   * 小时分钟 转化为  一天中的分钟数
   * {{{
   *   # 10点24 转化为当天的分钟数
   *   hourMinute2MinuteOfDay(lit("1024"))
   * }}}
   *
   * @return Int Column
   */
  def hour_min_to_min_of_day(colIn: Column): Column = hour_min_get_hour(colIn) * 60 + colIn.substr(3, 2).cast("int")


  /**
   * 分钟小时获取小时列
   *
   * @param colIn 分钟小时列
   * @return
   */
  def hour_min_get_hour(colIn: Column): Column = colIn.substr(1, 2).cast("int")

  /**
   * 一天中的分钟数 转化为 小时分钟
   * {{{
   *   # 10点24 转化为当天的分钟数
   *   hourMinute2MinuteOfDay(lit("1024"))
   * }}}
   *
   * @return String Column
   */
  def min_of_day_to_hour_min(colIn: Column): Column = concat(lpad((colIn / 60).cast("int"), 2, "0"), lpad(colIn % 60, 2, "0"))


  /** *
   *
   * 需要做一点 array 合并的 工作 ，重复的
   * 最好可以增加 输入Column 类型的判断
   * 2021.6.17
   * //todo 存在一个bug 当 输入的首个 head 为null 时 combine的结果为null
   */
  @tailrec
  @varargs
  final def array_combine(head: Column, tail: Column*): Column = {
    if (tail.isEmpty) {
      head
    } else {
      val next = tail.head
      val new_head = when(next.isNull, head).otherwise(flatten(array(head, next)))
      array_combine(new_head, tail.tail: _*)
    }
  }

  /** 将多列 string 按照顺序concat to one
   */
  def sort_str_concat(sepCol: Column, cols: Column*): Column = {
    import org.apache.spark.sql.functions.array_sort
    val array_cols = array_sort(array(cols: _*))
    val res = (0 until cols.size).map(x => array_cols.getItem(x))
    val cols_final: Seq[Column] = C.sep(sepCol, res)
    concat(cols_final: _*)
  }
}
