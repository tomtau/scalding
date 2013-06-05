package prototype

import org.scalatest.FunSuite


class TestMain extends FunSuite {
  
  test("basic test of dynamic programming optimization of a matrix multiplication chain") {
    val p = Array(30, 35, 15, 5, 10, 20, 25)
    val result = Main.MemMatrix(p)
    expect(15125) {result._1}
    // ((A1(A2 A3))((A4 A5) A6)
    expect(Main.MultOp(Main.MultOp(Main.Matrix(1), Main.MultOp(Main.Matrix(2),Main.Matrix(3))),
        Main.MultOp(Main.MultOp(Main.Matrix(4),Main.Matrix(5)),Main.Matrix(6))
        )
        ) {result._2}
  }
  
}