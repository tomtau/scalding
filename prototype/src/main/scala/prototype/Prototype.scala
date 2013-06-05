package prototype

object Main {

  abstract class MatrixChain
  case class MultOp(left: MatrixChain, right: MatrixChain) extends MatrixChain
  case class Matrix(id: Int) extends MatrixChain  
  
  /**
   * p - array with dimensions of the chain of matrices
   * p_{i − 1} × p_{i} is multiplied by p_{i} × p_{i + 1} 
   */
  def MemMatrix(p: Array[Int]): (Int, MatrixChain) = {
    // costs array for each subchain
    val m = Array.fill(p.length, p.length)(-1)
    // split marker array
    val s = Array.fill(p.length, p.length)(-1)
    
    def MemMatrixChain(p: Array[Int], i: Int, j: Int): Int = {
      if (m(i)(j) != -1) m(i)(j)
      if (i == j) m(i)(j) = 0
      else {
        m(i)(j) = Int.MaxValue
        for (k <- i to (j - 1)) {
          val cost = MemMatrixChain(p, i, k) + MemMatrixChain(p, k + 1, j) + p(i-1)*p(k)*p(j)
          if (cost < m(i)(j)) { 
            m(i)(j) = cost
            s(i)(j) = k
          }
        }
      }
      
      m(i)(j)
    }
    
    def MultOrder(i: Int, j: Int): MatrixChain = {
      if (i == j) Matrix(i)
      else {
        val k = s(i)(j)
        val X = MultOrder(i, k)
        val Y = MultOrder(k+1, j)
        MultOp(X, Y)
      }
      
    }
    
    val x = MemMatrixChain(p, 1, p.length - 1)

    (x, MultOrder(1, p.length - 1))
  }

}