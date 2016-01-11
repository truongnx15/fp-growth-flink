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

class PFPGrowthTest  {

  val minSupport = List[Double](0.3, 0.2, 0.25, 0.15, 0.1)
  val numItems = List[Int](10, 50, 70, 100, 150)
  val numTransactions = List[Int](20, 30, 1000, 2000, 5000)
  val itemDelimiter = " "
  val transactionFile: String = "transactions.txt"
  //val transactionFile: String = "sample_fpgrowth_local.txt"

  def generateTransactionFile(testNum: Int): Unit = {
    val random = Random
    val writer = new PrintWriter( transactionFile , "UTF-8")

    for(numTrans <- 0 to numTransactions(testNum)) {
      if (numTrans > 0) {
        writer.write("\n")
      }
      //Store items in the transaction
      var items: Set[String] = Set()

      var tranLength: Int = random.nextInt(numItems(testNum) * 3/4) + 1

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
    val transactions = IOHelper.readInput(env, transactionFile, itemDelimiter)
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
    val transactions = IOHelper.readInput(env, transactionFile, itemDelimiter)
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
    * @param thatMode
    */


  def compareModel(thisModel: ( ListBuffer[Set[String]], String), thatMode: (ListBuffer[Set[String]], String)) : Unit = {
    //println(s"Number of frequent itemsets  ${thisModel._2}: ${thisModel._1.size}")
    //println(s"Number of frequent itemsets  ${thatMode._2}: ${thatMode._1.size}")

    assert(thisModel._1.size == thatMode._1.size, "Number of frequent itemsets are different: " + thisModel._2 + " vs " + thatMode._2)
    assert(thisModel._1.toSet.sameElements(thatMode._1.toSet), "Frequent itemsets of are different: " + thisModel._2 + " vs " + thatMode._2)
  }

  @Test
  def testWithSpark: Unit = {

    //FLINK init
    val env = ExecutionEnvironment.getExecutionEnvironment
    val transactionsFlink = IOHelper.readInput(env, transactionFile, itemDelimiter)

    //SPARK init

    //This is a workout on windows to run spark locally. Set the hadoop.home.dir to your home hadoop folder
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common")

    val conf = new SparkConf().setAppName("PFPGrowth")setMaster("local[2]")
    val sc = new SparkContext(conf)

    for(testNum <- 0 to (minSupport.size - 1)) {

      println("TEST: " + (testNum + 1))
      println("transactions: " + numTransactions(testNum) + " max number of Items: " + numItems(testNum) + " : minSupport: " + minSupport(testNum))

      generateTransactionFile(testNum)

      val transactionsSpark = sc.textFile(transactionFile).map(_.split(" ")).cache()

      val modelSpark = new FPGrowth()
        .setMinSupport(minSupport(testNum))
        //.setNumPartitions(params.numPartition)
        .run(transactionsSpark)


      //TODO: FLINK val flinkModel = new PFPGrowth(env, -1, minSupport(testNum)).run(transactionsFlink)

      val localFPGrowthModel = testFPGrowthLocal(testNum)

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

      /*
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
      */

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

        println(s"Number of frequent itemsets BRUTE: ${modelBruteForce.size}")

        assert(modelSpark.freqItemsets.count() == modelBruteForce.size)
        //TODO: When flink implementation finished assert(modelFlink.count() == modelBruteForce.size)

        var frequentSetsBruteForce: ListBuffer[Set[String]] = new ListBuffer()
        modelBruteForce.foreach {
          itemset => frequentSetsBruteForce += itemset._1
        }

        compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsBruteForce, "BRUTE FORCE"))
        compareModel((frequentSetsSpark, "SPARK") , (frequentSetsBruteForce, "BRUTE FORCE"))
        //TODO: FLINK compareModel((frequentSetsFlink, "FLINK") , (frequentSetsBruteForce, "BRUTE FORCE"))
      }

      compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsSpark, "SPARK"))
      //TODO: FLINK compareModel((frequentSetsLocalFPGrowth, "LocalFPGrowth") , (frequentSetsFlink, "FLINK"))

      //TODO: FLINK compareModel((frequentSetsFlink, "frequentSetsFlink") , (frequentSetsFlink, "SPARK"))
    }
  }
}
