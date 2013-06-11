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

  def genFormula: Gen[MatrixFormula] = frequency((3, genLeaf), (1, genNode))  
  
  implicit def arbT: Arbitrary[MatrixFormula] = Arbitrary(genFormula)

  val genDimList = for {
    v <- Gen.choose(2, 7)
    dim <- Gen.listOfN(v, Gen.choose(1, 1000000000))
  } yield dim

  def generateRandomPlan(i: Int, j: Int, p: IndexedSeq[Literal]): MatrixFormula = {
    def genRoutine(k: Int): MatrixFormula = {
      val X = generateRandomPlan(i, k, p)
      val Y = generateRandomPlan(k + 1, j, p)
      Product(X, Y)      
    }
    
    if (i == j) p(i)
    else {
      val genK = Gen.choose(i, j)
      genK.sample match {
        case None => genRoutine(i)
        case Some(x) => genRoutine(x)
      }

    }

  }
  
  // ((A1(A2 A3))((A4 A5) A6)
  val optimizedPlan = Product( Product( Literal((30, 35), 1.0f), Product( Literal((35, 15), 1.0f), Literal((15, 5), 1.0f))),
         Product( Product( Literal((5, 10), 1.0f), Literal((10, 20), 1.0f)), Literal((20, 25), 1.0f)))

  val optimizedPlanCost = 15125.0

  // A1(A2(A3(A4(A5 A6))))
  val unoptimizedPlan = Product(Literal((30, 35), 1.0f), 
      Product(Literal((35, 15), 1.0f),
          Product(Literal((15, 5), 1.0f),
              Product(Literal((5, 10), 1.0f),
                  Product(Literal((10, 20), 1.0f), Literal((20, 25), 1.0f))
              )
          )
      ))

  val simplePlan = Product(Literal((30, 35), 1.0f), Literal((35, 25), 1.0f))

  val simplePlanCost = 26250

  val combinedUnoptimizedPlan = Sum(unoptimizedPlan, simplePlan)
  
  val combinedOptimizedPlan = Sum(optimizedPlan, simplePlan)
  
  val combinedOptimizedPlanCost = optimizedPlanCost + simplePlanCost
  
  val productSequence = IndexedSeq(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f),
        Literal((15, 5), 1.0f), Literal((5, 10), 1.0f), Literal((10, 20), 1.0f),
        Literal((20, 25), 1.0f))

  val combinedSequence = List(IndexedSeq(Literal((30, 35), 1.0f), Literal((35, 15), 1.0f),
        Literal((15, 5), 1.0f), Literal((5, 10), 1.0f), Literal((10, 20), 1.0f),
        Literal((20, 25), 1.0f)), IndexedSeq(Literal((30, 35), 1.0f), Literal((35, 25), 1.0f)))   
         
  test("base case") {
    val p = IndexedSeq(Literal((30, 35), 1.0f))
    val result = optimizeProductChain(p)
    expect((0.0, Literal((30, 35), 1.0f))) {result}
  }  
  
  test("only two matrices") {
    val p = IndexedSeq(Literal((30, 35), 1.0f), Literal((35, 25), 1.0f))
    val result = optimizeProductChain(p)
    expect((simplePlanCost, simplePlan)) {result}
  }
  
  test("basic test of dynamic programming optimization of a matrix multiplication chain") {
    val result = optimizeProductChain(productSequence)

    expect((optimizedPlanCost, optimizedPlan)) {result}
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

  test("optimized sum matrix formula to a sequence") {
    val result = matrixFormulaToChains(combinedOptimizedPlan)
    expect(combinedSequence.length) {result.length}
    for {
      i <- 0 to (combinedSequence.length - 1)
      j <- 0 to (combinedSequence(i).length - 1)
    } yield expect(combinedSequence(i)(j)) {result(i)(j)}    
  }

  test("unoptimized sum matrix formula to a sequence") {
    val result = matrixFormulaToChains(combinedUnoptimizedPlan)
    expect(combinedSequence.length) {result.length}
    for {
      i <- 0 to (combinedSequence.length - 1)
      j <- 0 to (combinedSequence(i).length - 1)
    } yield expect(combinedSequence(i)(j)) {result(i)(j)}    
  }
  
  test("optimizing an optimized plan") {
    expect((optimizedPlanCost, optimizedPlan)) {optimize(optimizedPlan)}
  }

  test("optimizing an unoptimized plan") {
    expect((optimizedPlanCost, optimizedPlan)) {optimize(unoptimizedPlan)}
  }  

  test("optimizing an optimized plan with sum") {
    expect((combinedOptimizedPlanCost,combinedOptimizedPlan)) {optimize(combinedOptimizedPlan)}
  }
  
  test("optimizing an unoptimized plan with sum") {
    expect((combinedOptimizedPlanCost, combinedOptimizedPlan)) {optimize(combinedUnoptimizedPlan)}
  }
    
  test("scalacheck: optimizing an optimized plan does not change it") {
    check((a: MatrixFormula) => optimize(a) == optimize(optimize(a)._2))
  }

  test("evaluate returns correct cost for an optimized plan") {
    expect((optimizedPlanCost, (30,25), 1.0f)) {evaluate(optimizedPlan)}
  }

  test("evaluate returns correct cost for a simple plan") {
    expect((simplePlanCost, (30,25), 1.0f)) {evaluate(simplePlan)}
  }
  
  test("evaluate returns correct cost for a combined optimized plan") {
    expect((combinedOptimizedPlanCost, (30,25), 1.0f)) {evaluate(combinedOptimizedPlan)}
  }
  
  test("scalacheck: testing evaluate") {
    check((a: MatrixFormula) => optimize(a)._1 == evaluate(optimize(a)._2)._1)
  }  

}