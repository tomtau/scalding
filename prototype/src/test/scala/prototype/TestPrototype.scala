package prototype

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck._

import org.scalacheck.Gen._


import prototype.Prototype._

class TestPrototype extends FunSuite with Checkers {

  val genLeaf = Literal((5,5), 1.0f)

  val genNode = for {
    v <- arbitrary[Int]
    left <- genFormula
    right <- genFormula
  } yield if (v > 0) Product(left, right) else Sum(left, right)

  def genFormula: Gen[MatrixFormula] = oneOf(genLeaf, genNode)  
  
  implicit def arbT: Arbitrary[MatrixFormula] = Arbitrary(genFormula)
  
  // ((A1(A2 A3))((A4 A5) A6)
  val optimizedPlan = Product( Product( Literal((30, 35), 1.0f), Product( Literal((35, 15), 1.0f), Literal((15, 5), 1.0f))),
         Product( Product( Literal((5, 10), 1.0f), Literal((10, 20), 1.0f)), Literal((20, 25), 1.0f)))
  
  // A1(A2(A3(A4(A5 A6))))
  val unoptimizedPlan = Product(Literal((30, 35), 1.0f), 
      Product(Literal((35, 15), 1.0f),
          Product(Literal((15, 5), 1.0f),
              Product(Literal((5, 10), 1.0f),
                  Product(Literal((10, 20), 1.0f), Literal((20, 25), 1.0f))
              )
          )
      ))
         
  val productSequence = Array(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f),
        Literal((15, 5), 1.0f), Literal((5, 10), 1.0f), Literal((10, 20), 1.0f),
        Literal((20, 25), 1.0f))
         
  test("base case") {
    val p = Array(Literal((30, 35), 1.0f))
    val result = optimizeProductChain(p)
    expect((0.0, Literal((30, 35), 1.0f))) {result}
  }  
  
  test("only two matrices") {
    val p = Array(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f))
    val result = optimizeProductChain(p)
    expect((15750.0, Product(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f)))) {result}
  }
  
  test("basic test of dynamic programming optimization of a matrix multiplication chain") {
    val result = optimizeProductChain(productSequence)

    expect((15125.0, optimizedPlan)) {result}
  }

  test("optimized matrix formula to a sequence") {
    val result = matrixFormulaToChains(optimizedPlan).head
    expect(productSequence.length) {result.length}
    for {
      i <- 0 to (productSequence.length - 1)
    } yield expect(productSequence(i)) {result(i)}    
  }

  test("unoptimized matrix formula to a sequence") {
    val result = matrixFormulaToChains(unoptimizedPlan).head
    expect(productSequence.length) {result.length}
    for {
      i <- 0 to (productSequence.length - 1)
    } yield expect(productSequence(i)) {result(i)}  
  }

  test("optimizing an optimized plan") {
    expect(optimizedPlan) {optimize(optimizedPlan)}
  }

  test("optimizing an unoptimized plan") {
    expect(optimizedPlan) {optimize(unoptimizedPlan)}
  }  
  
  test("scalacheck: optimizing an optimized plan does not change it") {
    check((a: MatrixFormula) => optimize(a) == optimize(optimize(a)))
  }
  
}