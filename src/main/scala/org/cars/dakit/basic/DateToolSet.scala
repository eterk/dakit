package org.cars.dakit.basic

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.annotation.tailrec

/**
 * 纯粹的日期时间转换工具
 */
object DateToolSet {

  def TODAY: LocalDate = LocalDate.now()

  def YESTERDAY: LocalDate = LocalDate.ofEpochDay(TODAY.toEpochDay - 1)

  val DATE8: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val DATE10: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val timestampToLocalDate = (x: Timestamp) => LocalDate.parse(x.toString.substring(0, 10), DATE10)

  /** 第一个一般是 最早的日期第二个日期一般是观察日期，第三个日期是最后的日期
   */
  type NatureDate = (LocalDate, LocalDate, LocalDate)
  /** 表示 一个时间范围
   */
  type DateRange = (LocalDate, LocalDate)
  type TimestampRange = (Timestamp, Timestamp)
  type TimestampNature = (Timestamp, Timestamp, Timestamp)

  /**
   * 一般观察日期当天包含有未发生的信息，所以为未来
   *
   * {{{
   *
   *   ((natureDate._1, natureDate._2.plusDays(-1)), (natureDate._2 -> natureDate._3))
   *
   * }}}
   *
   * @param natureDate
   * @return
   */
  def natureDateSplit(natureDate: NatureDate): (DateRange, DateRange) = {
    val res = rangeDateSplit(natureDate._1 -> natureDate._3, natureDate._2)
    (res._1.get, res._2.get)
  }

  def rangeDateSplit(target: DateRange, split: LocalDate): (Option[DateRange], Option[DateRange]) = {
    if (split.toEpochDay < target._1.toEpochDay || split.toEpochDay > target._2.toEpochDay) {
      (None, None)
    } else {
      (Option(target._1, split.minusDays(1)), Option(split, target._2))
    }
  }


  def toDate8(localDate: LocalDate): String = {
    localDate.format(DATE8)
  }

  def toTimestamp(localDate: LocalDate): Timestamp = {
    Timestamp.valueOf(localDate.format(DATE10) + " 00:00:00")
  }

  def toTimestampRange(dateRange: DateRange): TimestampRange = {
    (toTimestamp(dateRange._1), toTimestamp(dateRange._2))
  }

  def toTimestampNature(natureDate: NatureDate): TimestampNature = {
    (toTimestamp(natureDate._1), toTimestamp(natureDate._2), toTimestamp(natureDate._3))
  }

  /**
   * range 1 to range 2
   */
  def localDateSeq(dateRange: DateRange): Seq[LocalDate] = {
    dateRange._1.toEpochDay.to(dateRange._2.toEpochDay).map(LocalDate.ofEpochDay(_))
  }

  def timestampSeq(dateRange: DateRange): Seq[Timestamp] = {
    localDateSeq(dateRange).map(toTimestamp(_))
  }


  def dateRangeToStr(dateRange: DateRange) =
    dateRange.productIterator.map(_.asInstanceOf[LocalDate].format(DATE8)).mkString("_")

  def natureDateToStr(natureDate: NatureDate) =
    natureDate.productIterator.map(_.asInstanceOf[LocalDate].format(DATE8)).mkString("_")

  def normalNatureDate(year: Int, obDate: LocalDate, day: Int) = {
    (LocalDate.of(obDate.getYear - year, 1, 1), obDate, obDate.plusDays(day))
  }

  def getNatureDate(obDate: LocalDate, dayRange: (Int, Int)) = {
    (obDate.minusDays(dayRange._1), obDate, obDate.plusDays(dayRange._2))
  }

  def dateSplit(dateRange: (LocalDate, LocalDate), days: Int): Seq[DateRange] = {
    require(dateRange._1.toEpochDay <= dateRange._2.toEpochDay)
    require(days > 0)

    dateSplit(dateRange._1, dateRange._2, days, Seq.empty)
  }

  @tailrec
  private final def dateSplit(s: LocalDate, e: LocalDate, days: Int, res: Seq[DateRange]): Seq[DateRange] = {

    val cs = s.toEpochDay + days

    if (cs > e.toEpochDay) {
      return C(res, (s, e))
    }

    val newStart = LocalDate.ofEpochDay(cs)
    dateSplit(newStart, e, days, C(res, (s, newStart.minusDays(1))))
  }

}
