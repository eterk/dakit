package org.cars.dakit

import java.sql.Timestamp

import org.cars.dakit.test.TestHolder
import org.scalatest.FunSuite

case class Hello(time: Timestamp)


class MainTest extends FunSuite with TestHolder {

  import utils._

  test("x") {
    DF3.toCaseClass("Hello")


    DF3.to[Hello].show()
  }

}
