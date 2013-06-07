package prototype
import scala.collection.mutable.HashMap

object Prototype {

  sealed trait MatrixFormula
  case class Product(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula
  case class Sum(left: MatrixFormula, right: MatrixFormula) extends MatrixFormula
  case class Literal(id: Int) extends MatrixFormula

  /**
   * p - array with dimensions of the chain of matrices
   * p_{i − 1} × p_{i} is multiplied by p_{i} × p_{i + 1}
   */
  def memMatrix(p: Array[Int]): (Int, MatrixFormula) = {
    // costs array for each subchain
    val m = HashMap.empty[(Int,Int), Int]
    // split marker array
    val s = HashMap.empty[(Int,Int), Int]

    def memMatrixChain(p: Array[Int], i: Int, j: Int): Int = {
      if (m.contains((i,j))) m((i,j))
      if (i == j) m.put((i,j), 0)
      else {
        m.put((i,j), Int.MaxValue)
        for (k <- i to (j - 1)) {
          val cost = memMatrixChain(p, i, k) + memMatrixChain(p, k + 1, j) + p(i-1)*p(k)*p(j)
          if (cost < m((i,j))) {
            m.put((i,j), cost)
            s.put((i,j), k)
          }
        }
      }

      m((i,j))
    }

    def multOrder(i: Int, j: Int): MatrixFormula = {
      if (i == j) Literal(i)
      else {
        val k = s((i,j))
        val X = multOrder(i, k)
        val Y = multOrder(k + 1, j)
        Product(X, Y)
      }

    }

    val x = memMatrixChain(p, 1, p.length - 1)

    (x, multOrder(1, p.length - 1))
  }

}
