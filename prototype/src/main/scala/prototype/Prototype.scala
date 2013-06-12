package prototype
import scala.collection.mutable.HashMap

object Prototype {

  sealed trait MatrixFormula
  case class Product(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula
  case class Sum(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula
  case class Literal(dimensions: (Int, Int), sparsity: Float) extends MatrixFormula

  def optimizeProductChain(p: IndexedSeq[Literal]): (Double, MatrixFormula) = {

    val subchainCosts = HashMap.empty[(Int,Int), Double]

    val splitMarkers = HashMap.empty[(Int,Int), Int]

    def max(one: Float, two: Float, three: Float): Float = {
      if (one > two) {
        if (one > three) one else three
      } else {
        if (two > three) two else three
      }
    }

    val sparsities = p.map(x => x.sparsity)

    def computeCosts(p: IndexedSeq[Literal], i: Int, j: Int): Double = {
      if (subchainCosts.contains((i,j))) subchainCosts((i,j))
      if (i == j) subchainCosts.put((i,j), 0)
      else {
        subchainCosts.put((i,j), Double.MaxValue)
        for (k <- i to (j - 1)) {
          val ((rowsI, colsI), (rowsJ, colsJ)) = (p(i).dimensions, p(j).dimensions)
          val cost = computeCosts(p, i, k) + computeCosts(p, k + 1, j) +
                     rowsI * p(k).dimensions._2 * colsJ * sparsities.slice(i, j + 1).max
                     //max(p(i).sparsity, p(k).sparsity, p(j).sparsity)
          if (cost < subchainCosts((i,j))) {
            subchainCosts.put((i,j), cost)
            splitMarkers.put((i,j), k)
          }
        }
      }

      subchainCosts((i,j))
    }

    def generatePlan(i: Int, j: Int): MatrixFormula = {
      if (i == j) p(i)
      else {
        val k = splitMarkers((i,j))
        val X = generatePlan(i, k)
        val Y = generatePlan(k + 1, j)
        Product(X, Y)
      }

    }

    val best = computeCosts(p, 0, p.length - 1)

    (best, generatePlan(0, p.length - 1))
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
