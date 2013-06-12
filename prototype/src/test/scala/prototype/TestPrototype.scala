package prototype

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck._

import org.scalacheck.Gen._


import prototype.Prototype._

class TestPrototype extends FunSuite with Checkers {

  /**
   * Helper methods used in tests for randomized generations
   */
  def genLeaf(dims: (Int,Int)): Stream[Literal] = {
  	val (rows, cols) = dims
  	val sparGen = Gen.choose(0.0f, 1.0f)
  	val sparsity = sparGen.sample.get
  	val rowGen = Gen.choose(1, 1000)
  	val nextRows = if (rows <= 0) rowGen.sample.get else rows
  	if (cols <= 0) {
  		val colGen = Gen.choose(1, 1000)
  		val nextCols = colGen.sample.get
  		Literal((nextRows, nextCols), sparsity) #:: genLeaf((nextCols, 0))
  	} else {
  		Stream(Literal((nextRows, cols), sparsity))
  	}
  }

  def randomProduct(p: Int): MatrixFormula = {
    if (p == 1) genLeaf((5,5)).take(1)(0)
    val left = genLeaf((5,0)).take(p).toIndexedSeq
    val full = left ++ genLeaf((left.last.dimensions._2, 5)).toIndexedSeq
    generateRandomPlan(0, full.size - 1, full)
  }
  
  val genNode = for {
    v <- arbitrary[Int]
    p <- Gen.choose(1, 10)
    left <- genFormula
    right <- genFormula
  } yield if (v > 0) randomProduct(p) else Sum(left, right)

  def genFormula: Gen[MatrixFormula] = oneOf(genNode, genLeaf((5,5)).take(1).head)  
  
  implicit def arbT: Arbitrary[MatrixFormula] = Arbitrary(genFormula)

  val genProdSeq = for {
    v <- Gen.choose(1, 10)
  } yield genLeaf((0,0)).take(v).toIndexedSeq

  implicit def arbSeq: Arbitrary[IndexedSeq[Literal]] = Arbitrary(genProdSeq)
  
  def generateRandomPlan(i: Int, j: Int, p: IndexedSeq[Literal]): MatrixFormula = {
    if (i == j) p(i)
    else {
      val genK = Gen.choose(i, j - 1)
      val k = genK.sample.getOrElse(i)
      val X = generateRandomPlan(i, k, p)
      val Y = generateRandomPlan(k + 1, j, p)
      Product(X, Y) 
    }
  }
  
  /**
   * Values used in tests
   */
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

  /**
   * Basic "weak" test cases used in development
   */
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

  /**
   * Sanity check
   */
  test("scalacheck: optimizing an optimized plan does not change it") {
    check((a: MatrixFormula) => optimize(a) == optimize(optimize(a)._2))
  }

  /**
   * Function that recursively estimates a cost of a given MatrixFormula / plan.
   * This is the used in the tests for checking whether an optimized plan has
   * a cost <= a randomized plan.
   * The cost estimation of this evaluation should return the same values as the one
   * used in building optimized plans -- this is checked in the tests below.
   * @return (resulting cost, dimensions, sparsity)
   */
  def evaluate(mf: MatrixFormula): (Double, (Int, Int), Float) = {
    mf match {
      case element: Literal => (0.0, element.dimensions, element.sparsity)
      case Sum(left, right) => {
        val (costL, dimL, sparsL) = evaluate(left)
        val (costR, dimR, sparsR) = evaluate(right)
        (costL + costR, dimR, sparsL.max(sparsR))
      }
      case Product(left, right) => {
        val (costL, (rowsL, colsL), sparsL) = evaluate(left)
        val (costR, (rowsR, colsR), sparsR) = evaluate(right)
        (costL + costR + (rowsL * colsL * colsR * sparsL.max(sparsR)),
            (rowsL,colsR), sparsL.max(sparsR))
      }
    }
  }
    
  /**
   * Verifying "evaluate" function - that it does return
   * the same overall costs as what is estimated in the optimization procedure 
   */
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

  /**
   * "Proof": the goal property that estimated costs of optimized plans or product chains
   * are less than or equal to costs of randomized equivalent plans or product chains
   */
  test("scalacheck: testing costs of optimized chains") {
    check((a: IndexedSeq[Literal]) => optimizeProductChain(a)._1 <= evaluate(generateRandomPlan(0, a.length - 1, a))._1)
  }   

  test("scalacheck: testing costs of optimized plans versus random plans") {
    check((a: MatrixFormula) => optimize(a)._1 <= evaluate(a)._1)
  }   

  
}