package org.cars.dakit.basic

import scala.annotation.tailrec

object MapTool {


  def flattenMap[A, B](m: Map[A, Seq[B]]): Seq[Map[A, B]] = {
    val mSeq = m.toSeq
    val seqF = flattenSeq(mSeq)
    seqF.map(x => x.toMap)
  }

  def flattenSeq[A, B](s: Seq[(A, Seq[B])]): Seq[Seq[(A, B)]] = {
    flattenSeq(s, Seq.empty)
  }

  @tailrec
  final private[this] def flattenSeq[A, B](s: Seq[(A, Seq[B])], container: Seq[Seq[(A, B)]]): Seq[Seq[(A, B)]] = {
    if (s.isEmpty) {
      return container
    }
    val (ck, cv) = s.head

    val newC =
      if (container.isEmpty) {
        cv.map(x => Seq((ck, x)))
      } else {
        cv.flatMap(n => container.map(x => C(x, (ck, n))))
      }
    flattenSeq(s.tail, newC)
  }

}
