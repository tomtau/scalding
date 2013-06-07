package prototype

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._

import prototype.Prototype._

class TestPrototype extends FunSuite with Checkers {

  test("base case") {
    val p = Array(Literal((30, 35), 1.0f))
    val result = optimizeProductChain(p)
    expect(Literal((30, 35), 1.0f)) {result}
  }  
  
  test("only two matrices") {
    val p = Array(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f))
    val result = optimizeProductChain(p)
    expect(Product(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f))) {result}
  }
  
  test("basic test of dynamic programming optimization of a matrix multiplication chain") {
    val p = Array(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f),
        Literal((15, 5), 1.0f), Literal((5, 10), 1.0f), Literal((10, 20), 1.0f),
        Literal((20, 25), 1.0f))
    val result =  optimizeProductChain(p)

    // ((A1(A2 A3))((A4 A5) A6)
    expect( Product( Product( Literal((30, 35), 1.0f), Product( Literal((35, 15), 1.0f), Literal((15, 5), 1.0f))),
         Product( Product( Literal((5, 10), 1.0f), Literal((10, 20), 1.0f)), Literal((20, 25), 1.0f))
        )
        ) {result}
  }

/*  test("scalacheck") {
    check((a: List[Int], b: List[Int]) => a.size + b.size == (a ::: b).size)
  }
*/  
}