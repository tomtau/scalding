package prototype
import scala.collection.mutable.HashMap
import com.twitter.scalding.mathematics.NoClue
import com.twitter.scalding.mathematics.SizeHint
import com.twitter.scalding.mathematics.SparseHint
import com.twitter.scalding.mathematics.FiniteHint
import com.twitter.algebird.{ Monoid, Ring }

object Prototype {

  /**
   *  Definitions of the mockup lazily evaluated and self-optimized operations
   */
  sealed trait Matrix extends Iterable[(Int, Int, Double)] {
    def +(that: Matrix): Matrix = Sum(this, that)
    def *(that: Matrix): Matrix = Product(this, that)
    val sizeHint: SizeHint = NoClue
    lazy val optimizedSelf = optimize(this)._2
  }

  case class Product(left: Matrix, right: Matrix, optimal: Boolean = false)(implicit ring: Ring[Double]) extends Matrix {
    override val sizeHint = left.sizeHint * right.sizeHint

    def iterator: Iterator[(Int, Int, Double)] = toList().iterator

    override def toList(): List[(Int, Int, Double)] = {
      if (optimal) {
        // evaluate only if this product was already self-optimized
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
      } else {
        optimizedSelf.toList
      }
    }
  }

  case class Sum(left: Matrix, right: Matrix)(implicit mon: Monoid[Double]) extends Matrix {
    override val sizeHint = left.sizeHint + right.sizeHint

    def iterator: Iterator[(Int, Int, Double)] = toList().iterator

    override def toList(): List[(Int, Int, Double)] = {
      (left.optimizedSelf.view ++ right.optimizedSelf.view).groupBy(entry => (entry._1, entry._2)).toSeq.sortBy(entry => entry._1). // group entries
        map(joined => if (joined._2.size == 1) joined._2.head // sum or leave
        else (joined._2.head._1, joined._2.head._2, mon.plus(joined._2.head._3, joined._2.tail.head._3))).
        toList
    }
  }

  case class Literal(override val sizeHint: SizeHint, val vals: List[(Int, Int, Double)]) extends Matrix {
    def iterator: Iterator[(Int, Int, Double)] = vals.iterator

    override def toList(): List[(Int, Int, Double)] = vals
  }
  
  /**
   *  The original prototype that employs the standard O(n^3) dynamic programming
   *  procedure to optimize a matrix chain factorization
   */
  def optimizeProductChain(p: IndexedSeq[Matrix]): (Long, Matrix) = {

    val subchainCosts = HashMap.empty[(Int, Int), Long]

    val splitMarkers = HashMap.empty[(Int, Int), Int]

    def computeCosts(p: IndexedSeq[Matrix], i: Int, j: Int): Long = {
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
        val result = Product(left, right, true)
        result
      }

    }

    val best = computeCosts(p, 0, p.length - 1)

    (best, generatePlan(0, p.length - 1))
  }

  /**
   * This function walks the input tree, finds basic blocks to optimize,
   * i.e. matrix product chains that are not interrupted by summations.
   * One example:
   * A*B*C*(D+E)*(F*G) => "basic blocks" are ABC, D, E, and FG
   */
  def optimize(mf: Matrix): (Long, Matrix) = {

    /**
     * Helper function that either returns an optimized product chain
     * or the last visited place in the tree
     */
    def chainOrLast(chain: List[Matrix], last: Option[(Long, Matrix)]): (Long, Matrix) = {
      if (chain.isEmpty) last.get
      else optimizeProductChain(chain.toIndexedSeq)
    }

    /**
     * Recursive function - returns a flatten product chain so far and the rest of the connected tree
     */
    def optimizeBasicBlocks(mf: Matrix): (List[Matrix], Long, Option[(Long, Matrix)]) = {
      mf match {
        // basic block of one matrix
        case element: Literal => (List(element), 0, None)
        // two potential basic blocks connected by a sum
        case Sum(left, right) => {
          val (lastLChain, lastCost1, leftTemp) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, rightTemp) = optimizeBasicBlocks(right)
          val (cost1, newLeft) = chainOrLast(lastLChain, leftTemp)
          val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
          (List(Sum(newLeft, newRight)), lastCost1 + lastCost2 + cost1 + cost2, None)
        }
        // basic block A*B
        case Product(leftp: Literal, rightp: Literal, _) => {
          (List(leftp, rightp), 0, None)
        }
        // potential chain (...something...)*right or just two basic blocks connected by a product
        case Product(left: Product, right: Literal, _) => {
          val (lastLChain, lastCost, leftTemp) = optimizeBasicBlocks(left)
          if (lastLChain.isEmpty) {
            val (cost, newLeft) = leftTemp.get
            val interProduct = Product(newLeft, right, true)
            (Nil, cost + lastCost, Some(cost, interProduct))
          } else {
            (lastLChain ++ List(right), lastCost, leftTemp)
          }
        }
        // potential chain left*(...something...) or just two basic blocks connected by a product
        case Product(left: Literal, right: Product, _) => {
          val (lastRChain, lastCost, rightTemp) = optimizeBasicBlocks(right)
          if (lastRChain.isEmpty) {
            val (cost, newRight) = rightTemp.get
            val interProduct = Product(left, newRight, true)
            (Nil, cost + lastCost, Some(cost, interProduct))
          } else {
            (left :: lastRChain, lastCost, rightTemp)
          }
        }
        // potential chain (...something...)*(...something...) or just two basic blocks connected by a product
        case Product(left, right, _) => {
          val (lastLChain, lastCost1, leftTemp) = optimizeBasicBlocks(left)
          val (lastRChain, lastCost2, rightTemp) = optimizeBasicBlocks(right)
          if (lastLChain.isEmpty) {
            val (cost1, newLeft) = leftTemp.get
            val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
            (Nil, cost1 + cost2 + lastCost1 + lastCost2, Some(cost1 + cost2 + lastCost1 + lastCost2, Product(newLeft, newRight, true)))
          } else {
            if (lastRChain.isEmpty) {
              val (cost1, newLeft) = optimizeProductChain(lastLChain.toIndexedSeq)
              val (cost2, newRight) = rightTemp.get
              (Nil, cost1 + cost2 + lastCost1 + lastCost2, Some(cost1 + cost2 + lastCost1 + lastCost2, Product(newLeft, newRight, true)))
            } else {
              (lastLChain ++ lastRChain, lastCost1 + lastCost2, None)
            }
          }
        }
      }
    }
    val (lastChain, lastCost, form) = optimizeBasicBlocks(mf)
    val (potentialCost, finalResult) =  chainOrLast(lastChain, form)
    (lastCost + potentialCost, finalResult) 
  }

}
