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
  		(Literal(SparseHint(sparsity, nextRows, nextCols), globM), nextCols)
  	} else {
  		(Literal(SparseHint(sparsity, nextRows, cols), globM), cols)
  	}
  }

  val globM = matrix().sample.get

	def matrix(): Gen[List[(Int, Int, Double)]] = Gen.sized { size =>
	  val side = scala.math.sqrt(size).asInstanceOf[Int]
	  val g = arbitrary[Double]
	  Gen.listOfN(side, (Gen.choose(0, 1000).sample.get,Gen.choose(0, 1000).sample.get,g.sample.get))
	}  
  
  def productChainGen(current: Int, target: Int, prevCol: Long, result: List[Literal]): List[Literal] = {
    if (current == target) result
    else {
      val (randomMatrix, cols) = genLeaf((prevCol, 0))
      productChainGen(current + 1, target, cols, result ++ List(randomMatrix))
    }
  }
  
  def randomProduct(p: Int): Matrix = {
    if (p == 1) genLeaf((0,0))._1
    else {
      val full = productChainGen(0, p, 0, Nil).toIndexedSeq
      generateRandomPlan(0, full.size - 1, full)
    }
  }
  
  def genNode(depth: Int) = for {
    v <- arbitrary[Int]
    p <- Gen.choose(1, 10)
    left <- genFormula(depth + 1)
    right <- genFormula(depth + 1)
  } yield if (depth > 5) randomProduct(p) else (if (v > 0) randomProduct(p) else Sum(left, right))

  def genFormula(depth: Int): Gen[Matrix] = if (depth > 5) genLeaf((0,0))._1 else (oneOf(genNode(depth + 1), genLeaf((0,0))._1))  
  
  implicit def arbT: Arbitrary[Matrix] = Arbitrary(genFormula(0))

  val genProdSeq = for {
    v <- Gen.choose(1, 10)
  } yield productChainGen(0, v, 0, Nil).toIndexedSeq

  implicit def arbSeq: Arbitrary[IndexedSeq[Literal]] = Arbitrary(genProdSeq)
  
  def generateRandomPlan(i: Int, j: Int, p: IndexedSeq[Literal]): Matrix = {
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
  val optimizedPlan = Product(
		  				Product(Literal(FiniteHint(30, 35), globM),
		  						Product(Literal(FiniteHint(35, 15), globM),
		  						    Literal(FiniteHint(15, 5), globM),true),true),
		  						    Product(
		  						        Product(Literal(FiniteHint(5, 10), globM),
		  						        		Literal(FiniteHint(10, 20), globM), true),
		  						        Literal(FiniteHint(20, 25), globM), true), true)

  val optimizedPlanCost = 1300 //15125.0

  // A1(A2(A3(A4(A5 A6))))
  val unoptimizedPlan = Product(Literal(FiniteHint(30, 35), globM), 
      Product(Literal(FiniteHint(35, 15), globM),
          Product(Literal(FiniteHint(15, 5), globM),
              Product(Literal(FiniteHint(5, 10), globM),
                  Product(Literal(FiniteHint(10, 20), globM), Literal(FiniteHint(20, 25), globM))
              )
          )
      ))

  // A1 * (A2 * (A3 * ( A4 + A4 ) * (A5 * (A6))))

  val unoptimizedGlobalPlan = Product(Literal(FiniteHint(30, 35), globM), 
      Product(Literal(FiniteHint(35, 15), globM),
          Product(Literal(FiniteHint(15, 5), globM),
              Product(Sum(Literal(FiniteHint(5, 10), globM), Literal(FiniteHint(5, 10), globM)),
                  Product(Literal(FiniteHint(10, 20), globM), Literal(FiniteHint(20, 25), globM))
              )
          )
      ))      

  // ((A1(A2 A3))(((A4 + A4) A5) A6)
  val optimizedGlobalPlan = Product(
		  				Product(Literal(FiniteHint(30, 35), globM),
		  						Product(Literal(FiniteHint(35, 15), globM),
		  						    Literal(FiniteHint(15, 5), globM),true),true),
		  						    Product(
		  						        Product(Sum(Literal(FiniteHint(5, 10), globM),Literal(FiniteHint(5, 10), globM)),
		  						        		Literal(FiniteHint(10, 20), globM), true),
		  						        Literal(FiniteHint(20, 25), globM), true), true)      
      
  val simplePlan = Product(Literal(FiniteHint(30, 35), globM), Literal(FiniteHint(35, 25), globM), true)

  val simplePlanCost = 750 //26250

  val combinedUnoptimizedPlan = Sum(unoptimizedPlan, simplePlan)
  
  val combinedOptimizedPlan = Sum(optimizedPlan, simplePlan)
  
  val combinedOptimizedPlanCost = optimizedPlanCost + simplePlanCost
  
  val productSequence = IndexedSeq(Literal(FiniteHint(30, 35), globM), Literal(FiniteHint(35, 15), globM),
        Literal(FiniteHint(15, 5), globM), Literal(FiniteHint(5, 10), globM), Literal(FiniteHint(10, 20), globM),
        Literal(FiniteHint(20, 25), globM))

  val combinedSequence = List(IndexedSeq(Literal(FiniteHint(30, 35), globM), Literal(FiniteHint(35, 15), globM),
        Literal(FiniteHint(15, 5), globM), Literal(FiniteHint(5, 10), globM), Literal(FiniteHint(10, 20), globM),
        Literal(FiniteHint(20, 25), globM)), IndexedSeq(Literal(FiniteHint(30, 35), globM), Literal(FiniteHint(35, 25), globM)))   

  val planWithSum = Product(Literal(FiniteHint(30, 35), globM), Sum(Literal(FiniteHint(35, 25), globM), Literal(FiniteHint(35, 25), globM)), true)
        
  /**
   * Basic "weak" test cases used in development
   */
  test("base case") {
    val m = globM
    val p = IndexedSeq(Literal(FiniteHint(30, 35), m))
    val result = optimizeProductChain(p)
    expect((0.0, Literal(FiniteHint(30, 35), m))) {result}
  }  
  
  test("only two matrices") {
    val p = IndexedSeq(Literal(FiniteHint(30, 35), globM), Literal(FiniteHint(35, 25), globM))
    val result = optimizeProductChain(p)
    expect((simplePlanCost, simplePlan)) {result}
  }
  
  test("basic test of dynamic programming optimization of a matrix multiplication chain") {
    val result = optimizeProductChain(productSequence)

    expect((optimizedPlanCost, optimizedPlan)) {result}
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

  test("optimizing an unoptimized global plan") {
    expect(optimizedGlobalPlan) {optimize(unoptimizedGlobalPlan)._2}
  }  

  test("optimizing an optimized global plan") {
    expect(optimizedGlobalPlan) {optimize(optimizedGlobalPlan)._2}
  }    
  
  /**
   * Sanity checks
   */
  test("optimizing A*(B+C) doesn't break") {
    expect(planWithSum) {optimize(planWithSum)._2}
  }
  
  test("scalacheck: optimizing an optimized plan does not change it") {
    check((a: Matrix) => optimize(a) == optimize(optimize(a)._2))
  }

  /**
   * Function that recursively estimates a cost of a given MatrixFormula / plan.
   * This is the used in the tests for checking whether an optimized plan has
   * a cost <= a randomized plan.
   * The cost estimation of this evaluation should return the same values as the one
   * used in building optimized plans -- this is checked in the tests below.
   * @return resulting cost
   */
  def evaluate(mf: Matrix): Long = {
    
    /**
     * This function strips off the formula into a list of independent product chains
     * (i.e. same as matrixFormulaToChains in Prototype, but has Products
     * instead of IndexedSeq[Literal])
     */
    def toProducts(mf: Matrix): (Option[Product], List[Product]) = {
      mf match {
        case element: Literal => (None, Nil)
        case Sum(left, right) => {
          val (lastLP, leftR) = toProducts(left)
          val (lastRP, rightR) = toProducts(right)
          val total = leftR ++ rightR ++ (if (lastLP.isDefined) List(lastLP.get) else Nil) ++ 
          (if (lastRP.isDefined) List(lastRP.get) else Nil)
          (None, total)
        }
        case Product(leftp: Literal, rightp: Literal, _) => {
          (Some(Product(leftp, rightp)), Nil)
        }
        case Product(left: Product, right: Literal, _) => {
          val (lastLP, leftR) = toProducts(left)
          if (lastLP.isDefined) (Some(Product(lastLP.get, right)), leftR)
          else (None, leftR)
        }
        case Product(left: Literal, right: Product, _) => {
          val (lastRP, rightR) = toProducts(right)
          if (lastRP.isDefined) (Some(Product(left, lastRP.get)), rightR)
          else (None, rightR)
        }
        case Product(left, right, _) => {
          val (lastLP, leftR) = toProducts(left)
          val (lastRP, rightR) = toProducts(right)
          if (lastLP.isDefined && lastRP.isDefined) {
            (Some(Product(lastLP.get, lastRP.get)), leftR ++ rightR)
          } else {
            val newP = if (lastLP.isDefined) List(lastLP.get) else if (lastRP.isDefined) List(lastRP.get) else Nil 
            (None, newP ++ leftR ++ rightR)
          }
          
        }
      }
    }    
    
    /**
     * This function evaluates a product chain in the same way
     * as the dynamic programming procedure computes cost
     * (optimizeProductChain - computeCosts in Prototype)
     */
    def evaluateProduct(p: Matrix): Option[(Long, Matrix, Matrix)] = {
      p match {
        case Product(left: Literal, right: Literal, _) => {
          Some((left.sizeHint * (left.sizeHint * right.sizeHint)).total.get,
              left, right)
        }
        case Product(left: Literal, right: Product, _) => {
          val (cost, pLeft, pRight) = evaluateProduct(right).get
          Some(cost + (left.sizeHint * (left.sizeHint * pRight.sizeHint)).total.get,
              left, pRight)
        }
        case Product(left: Product, right: Literal, _) => {
          val (cost, pLeft, pRight) = evaluateProduct(left).get
          Some(cost + (pLeft.sizeHint * (pRight.sizeHint * right.sizeHint)).total.get,
              pLeft, right)
        }
        case Product(left: Matrix, right: Matrix, _) => {
          val (cost1, p1Left, p1Right) = evaluateProduct(left).get
          val (cost2, p2Left, p2Right) = evaluateProduct(right).get
          Some(cost1 + cost2 + (p1Left.sizeHint * (p1Right.sizeHint * p2Right.sizeHint)).total.get,
              p1Left, p2Right)
        }
        case _ => None
      }
    }
    
    val (last, productList) = toProducts(mf)
    val products = if (last.isDefined) last.get :: productList else productList
    products.map(p => evaluateProduct(p).get._1).sum
  }
    
  /**
   * Verifying "evaluate" function - that it does return
   * the same overall costs as what is estimated in the optimization procedure 
   */
  test("evaluate returns correct cost for an optimized plan") {
    expect(optimizedPlanCost) {evaluate(optimizedPlan)}
  }

  test("evaluate returns correct cost for a simple plan") {
    expect(simplePlanCost) {evaluate(simplePlan)}
  }
  
  test("evaluate returns correct cost for a combined optimized plan") {
    expect(combinedOptimizedPlanCost) {evaluate(combinedOptimizedPlan)}
  }
  
  test("scalacheck: testing evaluate") {
    check((a: Matrix) => optimize(a)._1 == evaluate(optimize(a)._2))
  }  

  /**
   * "Proof": the goal property that estimated costs of optimized plans or product chains
   * are less than or equal to costs of randomized equivalent plans or product chains
   */
  test("scalacheck: testing costs of optimized chains") {
    check((a: IndexedSeq[Literal]) => optimizeProductChain(a)._1 <= evaluate(generateRandomPlan(0, a.length - 1, a)))
  }   

  test("scalacheck: testing costs of optimized plans versus random plans") {
    check((a: Matrix) => optimize(a)._1 <= evaluate(a))
  }

  test("optimizing a strange random chain (that had a better cost)") {
    val chain = Vector(Literal(SparseHint(0.36482271552085876,940,325), globM), Literal(SparseHint(0.9494419097900391,325,545), globM), Literal(SparseHint(0.41427478194236755,545,206), globM), Literal(SparseHint(0.0032255554106086493,206,587), globM))
    val randomPlan = generateRandomPlan(0, chain.length - 1, chain)
    expect(true)(optimizeProductChain(chain)._1 <= evaluate(randomPlan))
  }
  
  test("optimizing a simplified plan from the above strange random chain (that had a better cost)") {
    val plan = Product(Product(Literal(SparseHint(0.4,900,30), globM),Product(Literal(SparseHint(1.0,30,50), globM),Literal(SparseHint(0.4,50,200), globM))),Literal(SparseHint(0.0003,200,500), globM))
    expect(true)(optimize(plan)._1 <= evaluate(plan))
  }
  
  test("optimizing a strange random plan (that had a better cost)") {
    val plan = Product(Product(Product(Product(Literal(SparseHint(0.15971194207668304,431,363), globM),Literal(SparseHint(0.7419577240943909,363,728), globM)),Product(Literal(SparseHint(0.7982533574104309,728,667), globM),Literal(SparseHint(1.9173489999957383E-4,667,677), globM))),Product(Literal(SparseHint(0.08173704147338867,677,493), globM),Literal(SparseHint(0.6515133380889893,493,623), globM))),Product(Literal(SparseHint(0.13034720718860626,623,450), globM),Product(Product(Literal(SparseHint(0.5519505739212036,450,496), globM),Literal(SparseHint(0.011094188317656517,496,478), globM)),Literal(SparseHint(0.21135291457176208,478,692), globM))))
    expect(true)(optimize(plan)._1 <= evaluate(plan))
  }
  
}