package prototype
import scala.collection.mutable.HashMap
import com.twitter.scalding.mathematics.NoClue
import com.twitter.scalding.mathematics.SizeHint
import com.twitter.scalding.mathematics.SparseHint
import com.twitter.scalding.mathematics.FiniteHint
import com.twitter.algebird.{ Monoid, Ring }

object Prototype {

  sealed trait Matrix extends Iterable[(Int, Int, Double)] {
    def +(that: Matrix): Matrix = Sum(this, that)
    def *(that: Matrix): Matrix = Product(this, that)
    val sizeHint: SizeHint = NoClue
  }

  case class Product(left: Matrix, right: Matrix)(implicit ring: Ring[Double]) extends Matrix {
    override val sizeHint = left.sizeHint * right.sizeHint

    def iterator: Iterator[(Int, Int, Double)] = toList().iterator

    override def toList(): List[(Int, Int, Double)] = {
      (for {
        keyL <- left.view
        keyR <- right.view
        if (keyL._2 == keyR._1)
      } yield ((keyL._1, keyR._2) -> (keyL, keyR))).
        groupBy(entry => entry._1).toSeq.sortBy(entry => entry._1). // get joined row-cols in order
        map(joined => (joined._1._1, joined._1._2, joined._2. // construct resulting entry
        map(els => ring.times(els._2._1._3, els._2._2._3)). // pairwise multiplication
        reduce((el1, el2) => ring.plus(el1, el2)))). // sum products to the resulting element
        toList
    }
  }

  case class Sum(left: Matrix, right: Matrix)(implicit mon: Monoid[Double]) extends Matrix {
    override val sizeHint = left.sizeHint + right.sizeHint

    def iterator: Iterator[(Int, Int, Double)] = toList().iterator

    override def toList(): List[(Int, Int, Double)] = {
      (left.view ++ right.view).groupBy(entry => (entry._1, entry._2)).toSeq.sortBy(entry => entry._1). // group entries
        map(joined => if (joined._2.size == 1) joined._2.head // sum or leave
        else (joined._2.head._1, joined._2.head._2, mon.plus(joined._2.head._3, joined._2.tail.head._3))).
        toList
    }
  }

  case class Literal(override val sizeHint: SizeHint, val vals: List[(Int, Int, Double)]) extends Matrix {
    def iterator: Iterator[(Int, Int, Double)] = vals.iterator

    override def toList(): List[(Int, Int, Double)] = vals
  }

  def optimizeProductChain(p: IndexedSeq[Literal]): (Long, Matrix) = {

    val subchainCosts = HashMap.empty[(Int, Int), Long]

    val splitMarkers = HashMap.empty[(Int, Int), Int]

    def computeCosts(p: IndexedSeq[Literal], i: Int, j: Int): Long = {
      if (subchainCosts.contains((i, j))) subchainCosts((i, j))
      if (i == j) subchainCosts.put((i, j), 0)
      else {
        subchainCosts.put((i, j), Long.MaxValue)
        for (k <- i to (j - 1)) {
          val cost = computeCosts(p, i, k) + computeCosts(p, k + 1, j) +
            (p(i).sizeHint * (p(k).sizeHint * p(j).sizeHint)).total.get
          if (cost < subchainCosts((i, j))) {
            subchainCosts.put((i, j), cost)
            splitMarkers.put((i, j), k)
          }
        }
      }

      subchainCosts((i, j))
    }

    def generatePlan(i: Int, j: Int): Matrix = {
      if (i == j) p(i)
      else {
        val k = splitMarkers((i, j))
        val left = generatePlan(i, k)
        val right = generatePlan(k + 1, j)
        Product(left, right)
      }

    }

    val best = computeCosts(p, 0, p.length - 1)

    (best, generatePlan(0, p.length - 1))
  }

  def matrixFormulaToChains(mf: Matrix): List[IndexedSeq[Literal]] = {

    def unflatten(result: List[Literal]): List[List[Literal]] = {
      if (result.isEmpty) Nil
      else List(result)
    }

    def toProductChains(mf: Matrix, result: List[Literal]): List[List[Literal]] = {
      mf match {
        case element: Literal => List(result ::: List(element))
        case Sum(left, right) => unflatten(result) ++ (toProductChains(left, Nil) ++ toProductChains(right, Nil))
        case Product(leftp, rightp) => {
          val left = toProductChains(leftp, result)
          left.slice(0, left.length - 2) ++ toProductChains(rightp, left.last)
        }
      }
    }
    toProductChains(mf, Nil).map(a => a.toIndexedSeq)
  }

  def optimize(mf: Matrix): (Double, Matrix) = {
    val optimizedChains = matrixFormulaToChains(mf)
    (optimizedChains.map(chain => optimizeProductChain(chain)._1).sum,
      optimizedChains.map(chain => optimizeProductChain(chain)._2).reduce((x, y) => Sum(x, y)))
  }

}
