import java.io.PrintWriter

import fpgrowth.{FPGrowth => FPGrowthLocal, Item}
import helper.{IOHelperFlink, IOHelper}
import org.apache.flink.api.scala._
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import pfp.{PFPGrowth, ParallelCounting}

import scala.collection.mutable.ListBuffer
import scala.util.Random


class TestPFPGrowth  {

  val minSupport = List[Double](0.1, 0.2, 0.3, 0.2, 0.25, 0.15, 0.15, 0.15)
  val numItems = List[Int](10, 15, 20, 50, 75, 110, 150, 150)
  val numTransactions = List[Int](15, 50, 100, 150, 1000, 2000, 3000, 5000)

  val maxBruteForceItems = 20

  //Maximum number of items to run local FPGrowth
  val maxLocalFPGrowthTransactions = 10000

  val itemDelimiter = " "
  val inputFolder = "testdata"

  var outputWriter: PrintWriter = _

  val numGroup: Int = 150

  val numParallism = 4

  /**
    * Randomly generate datasets in Unit Test
    * @param testNum the test number.
    */

  def generateTransactionFile(testNum: Int): Unit = {
    val random = Random
    val writer = new PrintWriter( getInputFileName(testNum) , "UTF-8")

    for(numTrans <- 0 until numTransactions(testNum)) {
      if (numTrans > 0) {
        writer.write("\n")
      }
      //Store items in the transaction
      var items: Set[String] = Set()

      //Transaction length
      val tranLength: Int = random.nextInt(numItems(testNum) * 3/4) + 1

      while (items.size < tranLength) {
        //Generate a letter for item
        val intNextItem = random.nextInt(numItems(testNum))
        val itemName = intNextItem.toString
        if (!items.contains(itemName)) {
          items += itemName

          if (items.size == 1) {
            writer.write(itemName)
          }
          else {
            writer.write(itemDelimiter + itemName)
          }
        }
      }
    }
    writer.close()
  }

  /**
    * Brute force algorithm to mine frequent itemset by generating all frequent itemsets
    * @param testNum test number
    * @return List of (itemset, frequency)
    */

  def bruteForceFrequentItemset(testNum: Int): ListBuffer[(Set[String], Long)] = {

    //Borrow read input from flink
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val transactions = IOHelperFlink.readInput(env, getInputFileName(testNum), itemDelimiter)
    val minCount = math.ceil(minSupport(testNum) * transactions.count()).toInt

    var allSetTransaction: ListBuffer[Set[String]] = ListBuffer()

    var allFrequentSetTransaction: ListBuffer[(Set[String], Long)] = ListBuffer()

    //Convert each transaction to a set
    transactions.collect().foreach {
      itemset => {
        var setOfItems: Set[String] = Set()
        itemset.foreach {
          x => setOfItems += x.name
        }
        allSetTransaction += setOfItems
      }
    }

    //Find distincts items
    val allItems: Seq[Item] = transactions
      .flatMap(ParallelCounting.ParallelCountingFlatMap)
      .groupBy(0)
      .reduceGroup(ParallelCounting.ParallelCountingGroupReduce)
      .collect()

    val frequentItems = allItems.filter( _.frequency >= minCount)

    //Bitmap indicating which items to get from list of items
    val possibleTransaction: Long = 1L << frequentItems.size

    //Build subset of frequentItems
    for(tranId <- 1L to (possibleTransaction - 1)) {
      var currentItemset: Set[String] = Set()
      for(j  <- 0 to frequentItems.size) {

        if ( ((tranId >> j) & 1L) == 1) {
          //Bit j of tranId = 1 => get item at position j
          currentItemset += frequentItems(j).name
        }
      }

      //Now we have a set of frequent itemset, we need to check if they are frequent
      var frequentCount: Long = 0
      allSetTransaction.foreach {
        itemset => if (itemset.intersect(currentItemset).size == currentItemset.size) frequentCount += 1
      }

      if (frequentCount >= minCount) {
        //This is frequentSet
        val tuple = (currentItemset, frequentCount)
        allFrequentSetTransaction += tuple
      }
    }

    //Return result
    allFrequentSetTransaction
  }

  /**
    * Run the local FPGrowth
    * @param testNum test number
    * @return List of frequent itemsets (itemset, frequency)
    */

  def testFPGrowthLocal(testNum: Int): ListBuffer[(ListBuffer[Item], Int)] = {

    //Read data
    val transactions = IOHelper.readInput(getInputFileName(testNum), itemDelimiter)
    val minCount: Long = math.ceil(minSupport(testNum) * transactions.size).toLong

    //Init and run FPGrowth
    val sorting: Boolean = true
    val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(transactions, minCount, sorting)

    //Result result
    fpGrowthLocal.getFrequentItemsets()
  }

  /**
    * Compare two model
    *
    * @param thisModel One model to compare
    * @param thatModel The other model to compare
    */


  def compareModel(thisModel: ( ListBuffer[Set[String]], String), thatModel: (ListBuffer[Set[String]], String)) : Unit = {
    outputWriter.write(s"Number of frequent itemsets  ${thisModel._2}: ${thisModel._1.size}" + "\n")
    outputWriter.write(s"Number of frequent itemsets  ${thatModel._2}: ${thatModel._1.size}" + "\n")

    print(s"Number of frequent itemsets  ${thisModel._2}: ${thisModel._1.size}" + "\n")
    print(s"Number of frequent itemsets  ${thatModel._2}: ${thatModel._1.size}" + "\n")

    //println(thisModel._2 + ": " + thisModel._1.toSet)
    //println(thatModel._2 + ": " + thatModel._1.toSet)
    //println(thisModel._1.toSet == thatModel._1.toSet)

    assert(thisModel._1.size == thatModel._1.size, "Number of frequent itemsets are different: " + thisModel._2 + " vs " + thatModel._2)
    assert(thisModel._1.toSet == thatModel._1.toSet, "Frequent itemsets of are different: " + thisModel._2 + " vs " + thatModel._2)
  }

  def getInputFileName(testNum: Int): String = {
    inputFolder + "/transactions-" + testNum + ".txt"
  }

  def getOutputFileName(testNum: Int): String = {
    inputFolder + "/transactions-" + testNum + "-result.txt"
  }

  /**
    * Run Spark FPGrowth
    * @param testNum test number
    * @return
    */
  def testSpeedSpark(testNum: Int) = {

    //This is a workout on windows to run spark locally. Set the hadoop.home.dir to your home hadoop folder
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common")
    val conf = new SparkConf().setAppName("PFPGrowth").setMaster("local[" + numParallism + "]")
    val sc = new SparkContext(conf)

    //Measuring time
    var startTime: Long = System.currentTimeMillis()

    //SPARK RUNNING
    val transactionsSpark = sc.textFile(getInputFileName(testNum), numParallism).map(_.split(" ")).cache()
    val modelSpark = new FPGrowth()
      .setMinSupport(minSupport(testNum))
      .setNumPartitions(numParallism)
      .run(transactionsSpark)

    //Collect itemsets
    val frequentSet = modelSpark.freqItemsets.collect()

    outputWriter.write("TEST: " + testNum + " - SPARK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")

    sc.stop()
    //return spark model
    frequentSet
  }

  /**
    * Run FLink FPGrowth
    * @param testNum test number
    * @return List[ListBuffer[Item], Frequency]
    */

  def testSpeedFlink(testNum: Int) = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(numParallism)

    val startTime = System.currentTimeMillis()

    //Run Flink FPGrowth
    val transactionsFlink = IOHelperFlink.readInput(env, getInputFileName(testNum), itemDelimiter)
    val pfp = new PFPGrowth(env, minSupport(testNum))
    pfp.numGroup = numGroup

    val flinkModel = pfp.run(transactionsFlink).collect()

    outputWriter.write("TEST: " + testNum + " - FLINK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")

    flinkModel
  }

  /**
    * The main test function
    */

  @Test
  def testCorrectness(): Unit = {

    for(testNum: Int <- minSupport.indices) {

      outputWriter = new PrintWriter( getOutputFileName(testNum) , "UTF-8")

      println("TEST: " + (testNum + 1))
      outputWriter.write("transactions: " + numTransactions(testNum) + " max number of Items: " + numItems(testNum) + " : minSupport: " + minSupport(testNum) + "\n")
      generateTransactionFile(testNum)

      //SPARK RUNNING
      val modelSpark = testSpeedSpark(testNum)

      //FLINK RUNNING
      val flinkModel = testSpeedFlink(testNum)

      //Extract frequentSet in Spark
      var frequentSetsSpark: ListBuffer[Set[String]] = new ListBuffer()
      modelSpark.foreach {
        itemset => {
          var currentFrequentSet: Set[String] = Set()
          itemset.items.foreach {
            item => currentFrequentSet += item
          }
          frequentSetsSpark += currentFrequentSet
        }
      }


      //Extract frequentSet in Flink
      var frequentSetsFlink: ListBuffer[Set[String]] = new ListBuffer()
      flinkModel.foreach {
        case (itemset, support) => {
          var currentFrequentSet: Set[String] = Set()
          itemset.foreach { item => currentFrequentSet += item.name}
          frequentSetsFlink += currentFrequentSet
        }
      }


      //Check conditional and run Local FPGrowth
      var frequentSetsLocalFPGrowth: ListBuffer[Set[String]] = new ListBuffer()
      if (numTransactions(testNum) <= maxLocalFPGrowthTransactions) {

        val startTime = System.currentTimeMillis()

        //LOCAL FPGROWTH RUNNING
        val localFPGrowthModel = testFPGrowthLocal(testNum)
        outputWriter.write("TEST: " + testNum + " - LOCAL FPGROWTH: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")

        //Extract frequentSet in local FPGrowth
        localFPGrowthModel.foreach{
          case (itemset, support) => {
            var currentFrequentSet: Set[String] = Set()
            itemset.foreach { item => currentFrequentSet += item.name}
            frequentSetsLocalFPGrowth += currentFrequentSet
          }
        }
      }

      //Check and run Brute Force algorithm
      if (numItems(testNum) <= maxBruteForceItems) {
        //Run bruteforce model
        var startTime = System.currentTimeMillis()
        val modelBruteForce = bruteForceFrequentItemset(testNum)
        outputWriter.write("TEST: " + testNum + " - BRUTE FORCE: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")

        var frequentSetsBruteForce: ListBuffer[Set[String]] = new ListBuffer()
        modelBruteForce.foreach {
          itemset => frequentSetsBruteForce += itemset._1
        }

        compareModel((frequentSetsSpark, "SPARK") , (frequentSetsBruteForce, "BRUTE FORCE"))
        compareModel((frequentSetsFlink, "FLINK") , (frequentSetsBruteForce, "BRUTE FORCE"))
        compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsBruteForce, "BRUTE FORCE"))
      }

      //Compare Spark and Flink
      if (numTransactions(testNum) <= maxLocalFPGrowthTransactions) {
        compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsSpark, "SPARK"))
        compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsFlink, "FLINK"))
      }

      compareModel((frequentSetsFlink, "frequentSetsFlink") , (frequentSetsFlink, "SPARK"))

      outputWriter.close()
    }
  }
}