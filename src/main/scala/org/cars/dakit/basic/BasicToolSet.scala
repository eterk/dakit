package org.cars.dakit.basic

import scala.collection.GenSeq

object  BasicToolSet {

  import scala.reflect.runtime.universe._

  /** 获取case 类的成员名字
   *
   * @tparam T 需要获取成员名字的 类
   * @return 这个成员名字的集合
   */
  def filedNames[T: TypeTag]: List[String] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m.name.toString
  }.toList.reverse


  def fieldsToMap[T](cc: T) = {
    (Map[String, AnyRef]() /: cc.getClass.getDeclaredFields) {
      (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(cc))
    }
  }

  /** 生成带小数的序列
   *
   * @param scale 有效小数位数
   * @return
   */
  def seqRange(from: Double, to: Double, by: Double, scale: Int): Array[Double] = {
    (BigDecimal(from) until BigDecimal(to) by BigDecimal(by)).toArray.map(_.formatted(s"%.${scale}f").toDouble)
  }

  def isNotDuplicated(seq: Seq[_]): Boolean = {
    seq.toSet.size == seq.size
  }


  /**
   * other's element is all in one
   *
   * {{{
   *   other.forall(y => one.exists(x => x == y)
   *
   * }}}
   *
   */
  def allIn(one: GenSeq[_], other: GenSeq[_]): Boolean = {
    one.intersect(other).length == other.length
  }

  /**
   * 将 system.nanotime 所获取的 时间差转化为人可读的近似的时间字符串，用来计算程序时间的
   */
  def smartTime(time: Long): String = {
    val ms_time = time / 1000 / 1000
    ms_time match {
      case ms if ms < 1000 => ms_time + "ms"
      case s if s < 1000 * 60 => ms_time / 1000 + "s"
      case _ => ms_time / 1000 / 60 + "min " + ms_time / 1000 % 60 + "s"
    }
  }

  def smartSize(byte: Long): String = {
    val unit = Seq("KB", "MB", "GB", "TB").filter(byteToOtherUnit(byte, _) > 1).head
    byteToOtherUnit(byte, unit).formatted(s"%.${3}f") + unit
  }

  def byteToOtherUnit(byte: Double, unit: String): Double = {
    unit.toUpperCase match {
      case "KB" => byte / 1024
      case "MB" => byte / 1024 / 1024
      case "GB" => byte / 1024 / 1024 / 1024
      case "TB" => byte / 1024 / 1024 / 1024 / 1024
    }
  }

  def time[T](f: => T): (T, Long) = {
    val s = System.nanoTime()
    val r = f
    val e = System.nanoTime()
    (r, e - s)
  }

}
