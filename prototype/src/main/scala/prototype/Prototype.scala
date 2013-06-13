package prototype
import scala.collection.mutable.HashMap
import com.twitter.scalding.mathematics.NoClue
import com.twitter.scalding.mathematics.SizeHint
import com.twitter.scalding.mathematics.SparseHint
import com.twitter.scalding.mathematics.FiniteHint

object Prototype {

  sealed trait MatrixFormula {
    val sizeHint : SizeHint = NoClue
  }
  case class Product(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula {
    override val sizeHint = left.sizeHint * right.sizeHint
  }
  case class Sum(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula {
    override val sizeHint = left.sizeHint + right.sizeHint
  }
  case class Literal(override val sizeHint: SizeHint) extends MatrixFormula

  def optimizeProductChain(p: IndexedSeq[Literal]): (Long, MatrixFormula) = {

    val subchainCosts = HashMap.empty[(Int,Int), Long]

    val splitMarkers = HashMap.empty[(Int,Int), Int]

    def computeCosts(p: IndexedSeq[Literal], i: Int, j: Int): Long = {
      if (subchainCosts.contains((i,j))) subchainCosts((i,j))
      if (i == j) subchainCosts.put((i,j), 0)
      else {
        subchainCosts.put((i,j), Long.MaxValue)
        for (k <- i to (j - 1)) {
          val cost = computeCosts(p, i, k) + computeCosts(p, k + 1, j) +
          (p(i).sizeHint * (p(k).sizeHint * p(j).sizeHint)).total.get
          if (cost < subchainCosts((i,j))) {
            subchainCosts.put((i,j), cost)
            splitMarkers.put((i,j), k)
          }
        }
      }

      subchainCosts((i,j))
    }

    def generatePlan(i: Int, j: Int): (Long, MatrixFormula) = {
      if (i == j) (0, p(i))
      else {
        val k = splitMarkers((i,j))
        val (costL, left) = generatePlan(i, k)
        val (costR, right) = generatePlan(k + 1, j)
        val result = Product(left, right)
        (costL + costR +result.sizeHint.total.get, result)
      }

    }

    computeCosts(p, 0, p.length - 1)

    generatePlan(0, p.length - 1)
  }

  def matrixFormulaToChains(mf: MatrixFormula): List[IndexedSeq[Literal]] = {

    def unflatten(result: List[Literal]): List[List[Literal]] = {
      if (result.isEmpty) Nil
      else List(result)
    }

    def toProductChains(mf: MatrixFormula, result: List[Literal]): List[List[Literal]] = {
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

  def optimize(mf: MatrixFormula): (Double, MatrixFormula) = {
    val optimizedChains = matrixFormulaToChains(mf)
    (optimizedChains.map(chain => optimizeProductChain(chain)._1).sum,
        optimizedChains.map(chain => optimizeProductChain(chain)._2).reduce((x,y) => Sum(x,y)))
  }

}
