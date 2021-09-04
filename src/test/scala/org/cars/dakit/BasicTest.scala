package org.cars.dakit

import org.cars.dakit.test.TestUtil
import org.scalatest.FunSuite

class BasicTest extends FunSuite with TestUtil {


  test("parseNames") {
    val originName = Seq("a", "a.a", "a.b", "a.c")
    val after = Seq("aN", "a.a1", "a.b1", "a.c1")
    val after1 = Seq("aN", "a1", "b1", "c1")


    // parent col name conflict
    require(isFail(parseNames(Seq("a", "a.b.d", "a.b.c"), Seq("aa9587", "a.d", "a.c"))))
    // conflict in a.b
    require(isFail(parseNames(Seq("a.a", "a.b.d", "a.b.c"), Seq("b1", "b2.d", "b3.c"))))
    // conflict in input duplicate  a.c
    require(isFail(parseNames(Seq("a.b", "a.c", "a.c"), Seq("a.b1", "a.c1", "ac2"))))
    // empty input rename
    require(isFail(parseNames(Seq("a.a", "a.b", "a.c"), Seq())))

    require(!isFail(parseNames(Seq("a.a", "a.b.d", "a.c.d"), Seq("b1", "b2", "b2"))))
    //  input duplicate  b.2
    require(isFail(parseNames(Seq("a.a", "a.b", "a.c"), Seq("b1", "b2", "b2"))))

  }
  test("originToResult") {
    require(originToResult("a.b.c", "ds.s") == "a.ds.s")
    require(originToResult("a.b.c", "s") == "a.b.s")
    require(originToResult("a.b.c", "d.e.f") == "d.e.f")
    require(originToResult("c", "f") == "f")
    require(isFail(originToResult("a.b.c", "")))
    require(isFail(originToResult("", "x")))
  }
  val strDistinct = (seq: Iterable[String]) => seq.toSet.size == seq.size

  def originToResult(before: String, after: String) = {
    require(after.nonEmpty)
    require(before.nonEmpty)
    val b: Seq[String] = before.split("\\.")
    val a: Seq[String] = after.split("\\.")
    require(b.size >= a.size)
    val x: Seq[String] = b.slice(0, b.size - a.size) ++ a ++ Nil
    x.mkString(".")
  }

  type StrPair = (String, String)


  def parent(a: String): String = {
    a.split("\\.").reverse.tail.reverse.mkString(".")
  }

  def numOfPoint(a: String): Int = {
    a.split("\\.").size - 1
  }

  def check(path: Seq[StrPair], nums: Int): Seq[StrPair] = {

    val f =
      path.map(v => {
        val res =
          if (numOfPoint(v._1) < nums) {
            (v._1, v._2)
          } else {
            (parent(v._1), parent(v._2))
          }
        println(nums + "--- " + res._1 + "   " + res._2)
        res
      })
    f.groupBy(x => x._1).foreach(l => {
      val p = l._2.toSet
      if (p.size != 1) {
        println(9527)
        throw new IllegalArgumentException(" conflict col names  " + p.mkString(","))
      }
    })
    if (nums == 0) {
      return path
    }

    check(f, nums - 1)
  }

  def parseNames(existsName: Seq[String], newName: Seq[String]): Unit = {
    require(existsName.size == newName.size)
    require(strDistinct(existsName), "can't have duplicate col")
    val existMap = existsName.zipWithIndex.toMap
    val newNameMap = newName.zipWithIndex.map(x => (x._2, x._1)).toMap

    val pathRoute: Map[String, String] = existsName
      .map(_.split("\\."))
      .groupBy(_.size)
      .flatMap(x => x._2.map(colPath => {
        val fullName = colPath.mkString(".")
        val rename = originToResult(fullName, newNameMap(existMap(fullName)))
        (fullName, rename)
      })).toMap
    check(pathRoute.toSeq, existsName.map(numOfPoint(_)).max)

    require(strDistinct(pathRoute.unzip._2.toSeq), "can't have duplicate col in same scope")


    "expect rename Result"


  }


}
