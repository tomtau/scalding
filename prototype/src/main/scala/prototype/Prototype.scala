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

    def generatePlan(i: Int, j: Int): MatrixFormula = {
      if (i == j) p(i)
      else {
        val k = splitMarkers((i,j))
        val left = generatePlan(i, k)
        val right = generatePlan(k + 1, j)
        Product(left, right)
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

  def matrixFormulaOptimize(mf: MatrixFormula): (Long, MatrixFormula) = {
		
		def chainOrLast(chain: List[Literal], last: Option[(Long, MatrixFormula)]): (Long, MatrixFormula) = {
			if (chain.isEmpty) last.get
			else optimizeProductChain(chain.toIndexedSeq)
		}
		
    def toProducts(mf: MatrixFormula): (List[Literal], Option[(Long, MatrixFormula)]) = {
      mf match {
        case element: Literal => (List(element), None)
        case Sum(left, right) => {
        	val (lastLChain, leftTemp) = toProducts(left)
          val (lastRChain, rightTemp) = toProducts(right)
          val (cost1, newLeft) = chainOrLast(lastLChain, leftTemp)
          val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
          (Nil, Some(cost1 + cost2, Sum(newLeft, newRight)))
        }
        case Product(leftp: Literal, rightp: Literal) => {
          (List(leftp, rightp), None)
        }
        case Product(left: Product, right: Literal) => {
          val (lastLChain, leftTemp) = toProducts(left)
          if (lastLChain.isEmpty) {
          		val (cost, newLeft) = leftTemp.get
          		(Nil, Some(cost, Product(newLeft, right)))
          } else {
          	(lastLChain ++ List(right), leftTemp)
          }
        }
        case Product(left: Literal, right: Product) => {
          val (lastRChain, rightTemp) = toProducts(right)
          if (lastRChain.isEmpty) {
          		val (cost, newRight) = rightTemp.get
          		(Nil, Some(cost, Product(left, newRight)))
          } else {
          	(left :: lastRChain, rightTemp)
          }
        }
        case Product(left, right) => {
          val (lastLChain, leftTemp) = toProducts(left)
          val (lastRChain, rightTemp) = toProducts(right)
          if (lastLChain.isEmpty) {
          	val (cost1, newLeft) = leftTemp.get
          	val (cost2, newRight) = chainOrLast(lastRChain, rightTemp)
          	(Nil, Some(cost1 + cost2, Product(newLeft, newRight)))
          } else {
          	if (lastRChain.isEmpty) {
          		val (cost1, newLeft) = optimizeProductChain(lastLChain.toIndexedSeq)
          		val (cost2, newRight) = rightTemp.get
          		(Nil, Some(cost1 + cost2, Product(newLeft, newRight)))
          	} else {
          		(lastLChain ++ lastRChain, None)
          	}
          }
        }
      }
    }
    val (lastChain, form) = toProducts(mf)
    
    chainOrLast(lastChain, form)
  }  
  
  def optimize(mf: MatrixFormula): (Long, MatrixFormula) = matrixFormulaOptimize(mf)

}
