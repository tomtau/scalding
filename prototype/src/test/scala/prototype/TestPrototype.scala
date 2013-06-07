package prototype

import org.scalatest.FunSuite


class TestMain extends FunSuite {
  
  test("only two matrices") {
    val p = Array(30, 35, 15)
    val result = Prototype.memMatrix(p)
    expect(15750) {result._1}
    expect(Prototype.MultOp(Prototype.Matrix(1), Prototype.Matrix(2))) {result._2}
  }  
  
  test("basic test of dynamic programming optimization of a matrix multiplication chain") {
    val p = Array(30, 35, 15, 5, 10, 20, 25)
    val result = Prototype.memMatrix(p)
    expect(15125) {result._1}
    // ((A1(A2 A3))((A4 A5) A6)
    expect(Prototype.MultOp(Prototype.MultOp(Prototype.Matrix(1), Prototype.MultOp(Prototype.Matrix(2),Prototype.Matrix(3))),
        Prototype.MultOp(Prototype.MultOp(Prototype.Matrix(4),Prototype.Matrix(5)),Prototype.Matrix(6))
        )
        ) {result._2}
  }
  
}