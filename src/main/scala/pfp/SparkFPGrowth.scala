package pfp

import helper.ParamHelper
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkContext, SparkConf}

object SparkFPGrowth {
  def main(args: Array[String]): Unit = {

    println("STARTING FPGROWTH IN SPARK")

    //Global variables for Flink and parameter parser
    val parameter = ParamHelper.parseArguments(args)
    val itemDelimiter = " "

    //Parse input parameter
    val input = parameter.getOrElse("--input", null)
    val minSupport = parameter.getOrElse("--support", null)
    val numGroup = parameter.getOrElse("--group", null)

    println("input: " + input + " support: " + minSupport + " numGroup: " + numGroup)

    if (input == null || input == "" || minSupport == null) {
      println("Please indicate input file and support: --input inputFile --support minSupport")
      return
    }

    if (numGroup != null && numGroup.toInt <= 0) {
      println("group parameter should be integer")
      return
    }

    //This is a workout on windows to run spark locally. Set the hadoop.home.dir to your home hadoop folder
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common")
    //val conf = new SparkConf().setAppName("SPARK PFPGrowth").setMaster("local[*]")
    val conf = new SparkConf().setAppName("SPARK PFPGrowth")
    val sc = new SparkContext(conf)

    val startTime: Long = System.currentTimeMillis()

    //SPARK RUNNING
    val transactionsSpark = sc.textFile(input).map(_.split(itemDelimiter)).cache()
    var modelSpark = new FPGrowth()
      .setMinSupport(minSupport.toDouble)
    if (numGroup != null) {
      modelSpark = modelSpark.setNumPartitions(numGroup.toInt)
    }

    val modelSparkResult = modelSpark.run(transactionsSpark)

    println("SPARK FPGrowth: " + modelSparkResult.freqItemsets.count())
    println("TIME SPARK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
    sc.stop()
  }
}
