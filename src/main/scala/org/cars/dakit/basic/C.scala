package org.cars.dakit.basic

/** 拼接 seq 或者单个字符
 *
 * @author K
 *         模仿R 语言中的c 功能 用以 flatMap
 *         使用方式
 *         C(Seq("a","b"),"c","d"):Seq[String]
 *         C(Seq(1,2),3,4):Seq[Int]
 *         C(c(1,2),3,4):Seq[Int]
 *         seq(1,2,3,4)
 *         C(c("a","v"),"c","3")
 *         > Seq("a","v","c","3")
 *
 *
 *         //todo  在这种情况下无法使用
 *         spark.emptyDataFrame.select(C("date","da"):_*)
 *
 */
object C extends Serializable {

  import scala.language.implicitConversions

  private[C] trait CMagnet[+A] {
    def get: Seq[A]

    override def toString = get.mkString(",")
  }

  private[C] object CMagnet {
    implicit def fromPlainValue[A](a: A): CMagnet[A] = new CMagnet[A] {
      def get: Seq[A] = a :: Nil
    }

    implicit def fromSeq[A](seq: Seq[A]): CMagnet[A] = new CMagnet[A] {
      def get: Seq[A] = seq
    }

    implicit def fromArray[A](arr: Array[A]): CMagnet[A] = new CMagnet[A] {
      def get: Seq[A] = arr.toSeq
    }

    implicit def fromList[A](list: List[A]): CMagnet[A] = new CMagnet[A] {
      def get: Seq[A] = list
    }
  }

  def apply[A](args: CMagnet[A]*): Seq[A] =
    args.flatMap(_.get)


  def sep[A](sep: A, args: Seq[A]): Seq[A] = {
    import collection.mutable.ArrayBuffer

    var first = true
    val result: ArrayBuffer[A] = ArrayBuffer.empty
    for (x <- args) {
      if (first) {
        result.append(x)
        first = false
      }
      else {
        result.append(sep)
        result.append(x)
      }
    }
    result
  }

}
