package prototype

import com.twitter.scalding.mathematics.SparseHint
import com.twitter.scalding.mathematics.SizeHint
import com.twitter.scalding.mathematics.NoClue
import com.twitter.algebird.{ Monoid, Group, Ring, Field }

object Mockup {

  sealed trait Matrix extends Iterable[Iterable[Tuple1[_]]] {
    def +(that: Matrix): Matrix = Sum(this, that)
    def *(that: Matrix): Matrix = Product(this, that)
    val sizeHint: SizeHint = NoClue
  }

  case class Product(left: Matrix, right: Matrix) extends Matrix {
    override val sizeHint = left.sizeHint * right.sizeHint

    def iterator: Iterator[Iterable[Tuple1[_]]] = List().iterator
  }
  case class Sum(left: Matrix, right: Matrix) extends Matrix {
    override val sizeHint = left.sizeHint + right.sizeHint

    def iterator: Iterator[Iterable[Tuple1[_]]] = List().iterator
  }
  case class Literal(override val sizeHint: SizeHint) extends Matrix {
    def iterator: Iterator[Iterable[Tuple1[_]]] = List().iterator
  }

  def main(args: Array[String]): Unit = {

  }

}