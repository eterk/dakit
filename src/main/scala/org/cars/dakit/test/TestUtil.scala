package org.cars.dakit.test

import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}

trait TestUtil {
  def isFail(f: => Unit) = {
    Try(f) match {
      case Success(v) => false
      case Failure(e) => true
    }
  }

  def isSameResult[T](f1: () => T, f2: () => T): Boolean = {

    f1() == f2()

  }

  private def sameDs(df1: Dataset[_], df2: Dataset[_]): Boolean = {
    df1.columns.size == df2.columns.size &&
      df1.join(df2, df1.columns, "left_semi").count() == df1.count()
  }

  def isSameDs(df1: Dataset[_], df2: Dataset[_]): Boolean = {
    val r = sameDs(df1, df2)
    if (!r) {
      df1.show()
      df2.show()
    } else {
      df1.show()
    }
    r
  }

}
