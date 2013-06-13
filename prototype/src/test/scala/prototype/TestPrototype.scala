package prototype

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck._
import org.scalacheck.Gen._
import prototype.Prototype._
import com.twitter.scalding.mathematics.SparseHint
import com.twitter.scalding.mathematics.FiniteHint
import com.twitter.scalding.mathematics.SizeHint

class TestPrototype extends FunSuite with Checkers {

  /**
   * Helper methods used in tests for randomized generations
   */
  def genLeaf(dims: (Long,Long)): (Literal, Long) = {
  	val (rows, cols) = dims
  	val sparGen = Gen.choose(0.0f, 1.0f)
  	val sparsity = sparGen.sample.get
  	val rowGen = Gen.choose(1, 1000)
  	val nextRows = if (rows <= 0) rowGen.sample.get else rows
  	if (cols <= 0) {
  		val colGen = Gen.choose(1, 1000)
  		val nextCols = colGen.sample.get
  		(Literal(SparseHint(sparsity, nextRows, nextCols)), nextCols)
  	} else {
  		(Literal(SparseHint(sparsity, nextRows, cols)), cols)
  	}
  }

  def productChainGen(current: Int, target: Int, prevCol: Long, result: List[Literal]): List[Literal] = {
    if (current == target) result
    else {
      val (randomMatrix, cols) = genLeaf((prevCol, 0))
      productChainGen(current + 1, target, cols, result ++ List(randomMatrix))
    }
  }
  
  def randomProduct(p: Int): MatrixFormula = {
    if (p == 1) genLeaf((0,0))._1
    else {
      val full = productChainGen(0, p, 0, Nil).toIndexedSeq
      generateRandomPlan(0, full.size - 1, full)
    }
  }
  
  val genNode = for {
    v <- arbitrary[Int]
    p <- Gen.choose(1, 10)
    left <- genFormula
    right <- genFormula
  } yield if (v > 0) randomProduct(p) else Sum(left, right)

  def genFormula: Gen[MatrixFormula] = oneOf(genNode, genLeaf((0,0))._1)  
  
  implicit def arbT: Arbitrary[MatrixFormula] = Arbitrary(genFormula)

  val genProdSeq = for {
    v <- Gen.choose(1, 10)
  } yield productChainGen(0, v, 0, Nil).toIndexedSeq

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
  val optimizedPlan = Product( Product( Literal(FiniteHint(30, 35)), Product( Literal(FiniteHint(35, 15)), Literal(FiniteHint(15, 5)))),
         Product( Product( Literal(FiniteHint(5, 10)), Literal(FiniteHint(10, 20))), Literal(FiniteHint(20, 25))))

  val optimizedPlanCost = 1300 //15125.0

  // A1(A2(A3(A4(A5 A6))))
  val unoptimizedPlan = Product(Literal(FiniteHint(30, 35)), 
      Product(Literal(FiniteHint(35, 15)),
          Product(Literal(FiniteHint(15, 5)),
              Product(Literal(FiniteHint(5, 10)),
                  Product(Literal(FiniteHint(10, 20)), Literal(FiniteHint(20, 25)))
              )
          )
      ))

  val simplePlan = Product(Literal(FiniteHint(30, 35)), Literal(FiniteHint(35, 25)))

  val simplePlanCost = 750 //26250

  val combinedUnoptimizedPlan = Sum(unoptimizedPlan, simplePlan)
  
  val combinedOptimizedPlan = Sum(optimizedPlan, simplePlan)
  
  val combinedOptimizedPlanCost = optimizedPlanCost + simplePlanCost
  
  val productSequence = IndexedSeq(Literal(FiniteHint(30, 35)), Literal(FiniteHint(35, 15)),
        Literal(FiniteHint(15, 5)), Literal(FiniteHint(5, 10)), Literal(FiniteHint(10, 20)),
        Literal(FiniteHint(20, 25)))

  val combinedSequence = List(IndexedSeq(Literal(FiniteHint(30, 35)), Literal(FiniteHint(35, 15)),
        Literal(FiniteHint(15, 5)), Literal(FiniteHint(5, 10)), Literal(FiniteHint(10, 20)),
        Literal(FiniteHint(20, 25))), IndexedSeq(Literal(FiniteHint(30, 35)), Literal(FiniteHint(35, 25))))   

  /**
   * Basic "weak" test cases used in development
   */
  test("base case") {
    val p = IndexedSeq(Literal(FiniteHint(30, 35)))
    val result = optimizeProductChain(p)
    expect((0.0, Literal(FiniteHint(30, 35)))) {result}
  }  
  
  test("only two matrices") {
    val p = IndexedSeq(Literal(FiniteHint(30, 35)), Literal(FiniteHint(35, 25)))
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
   * @return (resulting cost, SizeHint)
   */
  def evaluate(mf: MatrixFormula): (Double, SizeHint) = {
    mf match {
      case element: Literal => (0.0, element.sizeHint)
      case Sum(left, right) => {
        val (costL, leftHint) = evaluate(left)
        val (costR, rightHint) = evaluate(right)
        (costL + costR, leftHint + rightHint)
      }
      case Product(left, right) => {
        val (costL, leftHint) = evaluate(left)
        val (costR, rightHint) = evaluate(right)
        val newHint = leftHint * rightHint
        (costL + costR + newHint.total.get, newHint)
      }
    }
  }
    
  /**
   * Verifying "evaluate" function - that it does return
   * the same overall costs as what is estimated in the optimization procedure 
   */
  test("evaluate returns correct cost for an optimized plan") {
    expect((optimizedPlanCost, optimizedPlan.sizeHint)) {evaluate(optimizedPlan)}
  }

  test("evaluate returns correct cost for a simple plan") {
    expect((simplePlanCost, simplePlan.sizeHint)) {evaluate(simplePlan)}
  }
  
  test("evaluate returns correct cost for a combined optimized plan") {
    expect((combinedOptimizedPlanCost, combinedOptimizedPlan.sizeHint)) {evaluate(combinedOptimizedPlan)}
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

  test("optimizing a strange random chain (that had a better cost)") {
    val chain = Vector(Literal(SparseHint(0.36482271552085876,940,325)), Literal(SparseHint(0.9494419097900391,325,545)), Literal(SparseHint(0.41427478194236755,545,206)), Literal(SparseHint(0.0032255554106086493,206,587)))
    val randomPlan = generateRandomPlan(0, chain.length - 1, chain)
    expect(true)(optimizeProductChain(chain)._1 <= evaluate(randomPlan)._1)
  }
  
  test("optimizing a strange random plan (that had a better cost)") {
    val plan = Product(Product(Product(Product(Literal(SparseHint(0.15971194207668304,431,363)),Literal(SparseHint(0.7419577240943909,363,728))),Product(Literal(SparseHint(0.7982533574104309,728,667)),Literal(SparseHint(1.9173489999957383E-4,667,677)))),Product(Literal(SparseHint(0.08173704147338867,677,493)),Literal(SparseHint(0.6515133380889893,493,623)))),Product(Literal(SparseHint(0.13034720718860626,623,450)),Product(Product(Literal(SparseHint(0.5519505739212036,450,496)),Literal(SparseHint(0.011094188317656517,496,478))),Literal(SparseHint(0.21135291457176208,478,692)))))
    expect(true)(optimize(plan)._1 <= evaluate(plan)._1)
  }
  
}