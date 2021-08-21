package org.cars.dakit

import org.cars.dakit.basic.C
import org.scalatest.FunSuite

class CTest extends FunSuite {

  val toStr = (x: Seq[String]) => x.mkString(",")

  test("sep") {


    val seq0 = C(Seq(1, 2, 3), Seq(1, 2), List(3, 4))

    val b = Seq(1, 1, 2, 1, 3, 1, 1, 1, 2, 1, 3, 1, 4)

    val a = C.sep(1, seq0)

    require(a == b)

  }


}
