package prototype

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._

import prototype.Prototype._

class TestPrototype extends FunSuite with Checkers {
  
  test("only two matrices") {
    val p = Array(30, 35, 15)
    val result = memMatrix(p)
    expect(15750) {result._1}
    expect(Product(Literal(1), Literal(2))) {result._2}
  }  
  
  test("basic test of dynamic programming optimization of a matrix multiplication chain") {
    val p = Array(30, 35, 15, 5, 10, 20, 25)
    val result =  memMatrix(p)
    expect(15125) {result._1}
    // ((A1(A2 A3))((A4 A5) A6)
    expect( Product( Product( Literal(1),  Product( Literal(2), Literal(3))),
         Product( Product( Literal(4), Literal(5)), Literal(6))
        )
        ) {result._2}
  }

/*  test("scalacheck") {
    check((a: List[Int], b: List[Int]) => a.size + b.size == (a ::: b).size)
  }
*/  
}