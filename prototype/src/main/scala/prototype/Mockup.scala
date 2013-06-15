package prototype

import com.twitter.scalding.mathematics.SizeHint
import com.twitter.scalding.mathematics.NoClue
import com.twitter.algebird.{ Monoid, Ring }

object Mockup {

  sealed trait Matrix extends Iterable[Iterable[Double]] {
    def +(that: Matrix): Matrix = Sum(this, that)
    def *(that: Matrix): Matrix = Product(this, that)
    val sizeHint: SizeHint = NoClue
  }

  case class Product(left: Matrix, right: Matrix)(implicit ring: Ring[Double]) extends Matrix {
    override val sizeHint = left.sizeHint * right.sizeHint

    def iterator: Iterator[Iterable[Double]] = toList().iterator

    override def toList(): List[Iterable[Double]] = {
      (for {
        keyL <- left.view
        keyR <- right.view.transpose
      } yield (keyL, keyR)).zipWithIndex.groupBy(row => row._2 / left.size).toSeq.sortBy(a => a._1). // get joined row-cols in order
        map(x => x._2.map(y => y._1).map(z => (z._1 zip z._2). // strip off indices and zip entry elements
        map(els => ring.times(els._1, els._2)). // pairwise multiplication
        reduce((el1, el2) => ring.plus(el1, el2)))). // sum products to the resulting element
        toList
    }
  }

  case class Sum(left: Matrix, right: Matrix)(implicit mon: Monoid[Double]) extends Matrix {
    override val sizeHint = left.sizeHint + right.sizeHint

    def iterator: Iterator[Iterable[Double]] = toList().iterator

    override def toList(): List[Iterable[Double]] = {
      (left.view zip right.view) map (cols => (cols._1 zip cols._2) map (x => mon.plus(x._1, x._2))) toList
    }
  }

  case class Literal(override val sizeHint: SizeHint, val vals: List[List[Double]]) extends Matrix {
    def iterator: Iterator[Iterable[Double]] = vals.iterator

    override def toList(): List[List[Double]] = vals
  }
}