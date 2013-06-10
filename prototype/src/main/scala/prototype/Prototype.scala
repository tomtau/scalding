package prototype
import scala.collection.mutable.HashMap

object Prototype {

  sealed trait MatrixFormula
  case class Product(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula
  case class Sum(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula
  case class Literal(dimensions: (Int, Int), sparsity: Float) extends MatrixFormula

  def optimizeProductChain(p: IndexedSeq[Literal]): (Float, MatrixFormula) = {
    // costs for each subchain
    val m = HashMap.empty[(Int,Int), Float]
    // split markers
    val s = HashMap.empty[(Int,Int), Int]

    def max(one: Float, two: Float, three: Float): Float = {
      if (one > two) {
        if (one > three) one else three
      } else {
        if (two > three) two else three
      }
    }

    def computeCosts(p: IndexedSeq[Literal], i: Int, j: Int): Float = {
      if (m.contains((i,j))) m((i,j))
      if (i == j) m.put((i,j), 0)
      else {
        m.put((i,j), Int.MaxValue)
        for (k <- i to (j - 1)) {
          val cost = computeCosts(p, i, k) + computeCosts(p, k + 1, j) +
                     p(i).dimensions._1 * p(k).dimensions._2 * p(j).dimensions._2 *
                     max(p(i).sparsity, p(k).sparsity, p(j).sparsity)
          if (cost < m((i,j))) {
            m.put((i,j), cost)
            s.put((i,j), k)
          }
        }
      }

      m((i,j))
    }

    def generatePlan(i: Int, j: Int): MatrixFormula = {
      if (i == j) p(i)
      else {
        val k = s((i,j))
        val X = generatePlan(i, k)
        val Y = generatePlan(k + 1, j)
        Product(X, Y)
      }

    }

    val best = computeCosts(p, 0, p.length - 1)

    (best, generatePlan(0, p.length - 1))
  }

  def matrixFormulaToChains(mf: MatrixFormula): List[IndexedSeq[Literal]] = {

    def nilOrList(result: List[Literal]): List[List[Literal]] = {
      if (result.isEmpty) Nil
      else List(result)
    }

    def toProductChains(mf: MatrixFormula, result: List[Literal]): List[List[Literal]] = {
      mf match {
        case element: Literal => List(result ::: List(element))
        case sumOp: Sum => nilOrList(result) ++ toProductChains(sumOp.left, List()) ++ toProductChains(sumOp.right, List())
        case multOp: Product => {
          val left = toProductChains(multOp.left, result)
          left.slice(0, left.length - 2) ++ toProductChains(multOp.right, left.last)
        }
      }
    }
    toProductChains(mf, List()).map(a => a.toIndexedSeq)
  }

  def optimize(mf: MatrixFormula): MatrixFormula = {
    val optimizedChains = matrixFormulaToChains(mf).map(chain => optimizeProductChain(chain)._2)
    optimizedChains.reduce((x,y) => Sum(x,y))
  }

}
