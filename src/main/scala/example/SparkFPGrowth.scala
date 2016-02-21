package example

import helper.ParamHelper
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Class to run Spark FPGrowth. It is similar to the example at:
  * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/FPGrowthExample.scala
  * However, we observed that we should partition the input transactions equal to number of parallelism of achieve best performance
  * On datasets we test
  */

object SparkFPGrowth {
  def main(args: Array[String]): Unit = {

    println("STARTING FPGROWTH IN SPARK")

    //Global variables for Flink and parameter parser
    val parameter = ParamHelper.parseArguments(args)
    val itemDelimiter = " "

    //Parse input parameter
    val input = parameter.getOrElse("--input", null)
    val minSupport = parameter.getOrElse("--support", null)
    //Number of groups of items
    val numGroup = parameter.getOrElse("--group", null)

    println("input: " + input + " support: " + minSupport + " numGroup: " + numGroup.toInt)

    if (input == null || input == "" || minSupport == null) {
      println("Please indicate input file and support: --input inputFile --support minSupport")
      return
    }

    if (numGroup == null || numGroup.toInt <= 0) {
      println("group parameter must be set equal to parallelism level")
      return
    }

    //This is a workout on windows to run spark locally. Set the hadoop.home.dir to your home hadoop folder
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common")
    //val conf = new SparkConf().setAppName("SPARK PFPGrowth").setMaster("local[*]")
    val conf = new SparkConf().setAppName("SPARK PFPGrowth")
    val sc = new SparkContext(conf)
    sc.defaultParallelism

    val startTime: Long = System.currentTimeMillis()

    //SPARK RUNNING
    val data = sc.textFile(input, numGroup.toInt)
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' ')).cache()
    val fpg = new FPGrowth()
      .setMinSupport(minSupport.toDouble)

    val model = fpg.run(transactions)

    //val modelSparkResult = modelSpark.run(transactionsSpark)
    println("SPARK FPGROWTH: " + model.freqItemsets.count())
    println("TIME SPARK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
    sc.stop()
  }
}
