import java.io.PrintWriter

import fpgrowth.{FPGrowth => FPGrowthLocal, Itemset, Item}
import org.apache.flink.api.scala._
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import pfp.{IOHelper, PFPGrowth, ParallelCounting}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class TestPFPGrowth  {

  val minSupport = List[Double](0.3, 0.2, 0.25, 0.15, 0.2)
  val numItems = List[Int](10, 50, 70, 100, 150)
  val numTransactions = List[Int](20, 30, 1000, 2000, 3000)
  val itemDelimiter = " "
  val inputFolder = "testdata"
  var outputWriter: PrintWriter = _

  def generateTransactionFile(testNum: Int): Unit = {
    val random = Random
    val writer = new PrintWriter( getInputFileName(testNum) , "UTF-8")

    for(numTrans <- 0 to (numTransactions(testNum) - 1)) {
      if (numTrans > 0) {
        writer.write("\n")
      }
      //Store items in the transaction
      var items: Set[String] = Set()

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

  def bruteForceFrequentItemset(testNum: Int): ListBuffer[(Set[String], Long)] = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val transactions = IOHelper.readInput(env, getInputFileName(testNum), itemDelimiter)
    val minCount: Int = math.ceil(minSupport(testNum) * transactions.count()).toInt

    var allSetTransaction: ListBuffer[Set[String]] = ListBuffer()

    var allFrequentSetTransaction: ListBuffer[(Set[String], Long)] = ListBuffer()

    //Convert each transaction to a set
    val setTransactions = transactions.collect().foreach {
      itemset => {
        var setOfItems: Set[String] = Set()
        val items = itemset.getItems()
        items.foreach {
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

    return allFrequentSetTransaction
  }

  def testFPGrowthLocal(testNum: Int): ListBuffer[Itemset] = {

    //Employ flink and FPGrowth to read data
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val transactions = IOHelper.readInput(env, getInputFileName(testNum), itemDelimiter)
    val minCount: Long = math.ceil(minSupport(testNum) * transactions.count()).toLong

    //convert DataSet to ListBuffer
    var inputTransactions: ListBuffer[Itemset] = new ListBuffer[Itemset]()
    transactions.collect().flatMap(inputTransactions += _)

    //Init and run FPGrowth
    val sorting: Boolean = true
    val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(inputTransactions, minCount, sorting);

    return fpGrowthLocal.extractPattern(fpGrowthLocal.fptree, null)
  }

  /**
    * Compare two model
    * @param thisModel
    * @param thatModel
    */


  def compareModel(thisModel: ( ListBuffer[Set[String]], String), thatModel: (ListBuffer[Set[String]], String)) : Unit = {
    outputWriter.write(s"Number of frequent itemsets  ${thisModel._2}: ${thisModel._1.size}" + "\n")
    outputWriter.write(s"Number of frequent itemsets  ${thatModel._2}: ${thatModel._1.size}" + "\n")

    //println(thisModel._2 + ": " + thisModel._1.toSet)
    //println(thatModel._2 + ": " + thatModel._1.toSet)
    //println(thisModel._1.toSet == thatModel._1.toSet)

    assert(thisModel._1.size == thatModel._1.size, "Number of frequent itemsets are different: " + thisModel._2 + " vs " + thatModel._2)
    assert(thisModel._1.toSet == thatModel._1.toSet, "Frequent itemsets of are different: " + thisModel._2 + " vs " + thatModel._2)
  }

  def getInputFileName(testNum: Int): String = {
    return inputFolder + "/transactions-" + testNum + ".txt"
  }

  def getOutputFileName(testNum: Int): String = {
    return inputFolder + "/transactions-" + testNum + "-result.txt"
  }

  @Test
  def testSpeedSpark() : Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common")
    val testNum = 3
    val conf = new SparkConf().setAppName("PFPGrowth").setMaster("local[4]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    var startTime: Long = System.currentTimeMillis()

    //SPARK RUNNING
    val transactionsSpark = sc.textFile(getInputFileName(testNum)).map(_.split(" ")).cache()
    val modelSpark = new FPGrowth()
      .setMinSupport(minSupport(testNum))
      //.setNumPartitions(params.numPartition)
      .run(transactionsSpark)
    println("NUM FREQUENT SETS SPARK: " + modelSpark.freqItemsets.collect().size)
    println("TEST: " + testNum + " - SPARK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
    startTime = System.currentTimeMillis()
  }

  @Test
  def testSpeedFlink(): Unit = {
    val testNum = 3
    val inputFileName = getInputFileName(testNum)
    val env = ExecutionEnvironment.getExecutionEnvironment

    var startTime: Long = System.currentTimeMillis()
    val transactionsFlink = IOHelper.readInput(env, getInputFileName(testNum), itemDelimiter)
    val flinkModel = new PFPGrowth(env, minSupport(testNum)).run(transactionsFlink)

    println("NUM FREQUENT SETS FLINK: " + flinkModel.size)

    println("TEST: " + testNum + " - FLINK: " + (System.currentTimeMillis() - startTime)/1000.0)

  }

  @Test
  def testWithSpark: Unit = {

    //FLINK init
    val env = ExecutionEnvironment.getExecutionEnvironment


    //SPARK init

    //This is a workout on windows to run spark locally. Set the hadoop.home.dir to your home hadoop folder
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common")

    val conf = new SparkConf().setAppName("PFPGrowth")setMaster("local[2]")
    val sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()

    for(testNum <- 0 to (minSupport.size - 1)) {

      outputWriter = new PrintWriter( getOutputFileName(testNum) , "UTF-8")

      //println("TEST: " + (testNum + 1))
      outputWriter.write("transactions: " + numTransactions(testNum) + " max number of Items: " + numItems(testNum) + " : minSupport: " + minSupport(testNum) + "\n")
      //generateTransactionFile(testNum)

      //Mesure time for spark:
      var startTime: Long = System.currentTimeMillis()

      //SPARK RUNNING
      val transactionsSpark = sc.textFile(getInputFileName(testNum)).map(_.split(" ")).cache()
      val modelSpark = new FPGrowth()
        .setMinSupport(minSupport(testNum))
        //.setNumPartitions(params.numPartition)
        .run(transactionsSpark)
      outputWriter.write("TEST: " + testNum + " - SPARK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
      startTime = System.currentTimeMillis()


      //FLINK RUNNING
      val transactionsFlink = IOHelper.readInput(env, getInputFileName(testNum), itemDelimiter)
      val flinkModel = new PFPGrowth(env, minSupport(testNum)).run(transactionsFlink)
      outputWriter.write("TEST: " + testNum + " - FLINK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
      startTime = System.currentTimeMillis()

      //LOCAL FPGROWTH RUNNING
      val localFPGrowthModel = testFPGrowthLocal(testNum)
      outputWriter.write("TEST: " + testNum + " - LOCAL FPGROWTH: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
      startTime = System.currentTimeMillis()

      //Extract frequentSet in Spark
      var frequentSetsSpark: ListBuffer[Set[String]] = new ListBuffer()
      //val sparkFrequentSets = extractFrequentSetSpark(modelSpark)
      modelSpark.freqItemsets.collect().foreach {
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
        itemset => {
          var items = itemset.getItems()
          var currentFrequentSet: Set[String] = Set()
          items.foreach { item => currentFrequentSet += item.name}
          frequentSetsFlink += currentFrequentSet
        }
      }


      //Extract frequentSet in local FPGrowth
      var frequentSetsLocalFPGrowth: ListBuffer[Set[String]] = new ListBuffer()
      localFPGrowthModel.foreach{
        itemset => {
          var currentFrequentSet: Set[String] = Set()
          itemset.items.foreach { item => currentFrequentSet += item.name}
          frequentSetsLocalFPGrowth += currentFrequentSet
        }
      }

      if (numItems(testNum) <= 20) {
        //Run bruteforce model
        val modelBruteForce = bruteForceFrequentItemset(testNum)

        var frequentSetsBruteForce: ListBuffer[Set[String]] = new ListBuffer()
        modelBruteForce.foreach {
          itemset => frequentSetsBruteForce += itemset._1
        }

        compareModel((frequentSetsSpark, "SPARK") , (frequentSetsBruteForce, "BRUTE FORCE"))
        compareModel((frequentSetsFlink, "FLINK") , (frequentSetsBruteForce, "BRUTE FORCE"))
        compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsBruteForce, "BRUTE FORCE"))
      }

      compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsSpark, "SPARK"))
      compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsFlink, "FLINK"))

      compareModel((frequentSetsFlink, "frequentSetsFlink") , (frequentSetsFlink, "SPARK"))

      outputWriter.close()
    }
  }
}
