package pfp

import helper.ParamHelper
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkContext, SparkConf}

object SparkFPGrowth {
  def main(args: Array[String]): Unit = {

    println("STARTING FPGROWTH IN SPARK")

    //Global variables for Flink and parameter parser
    val parameter = ParamHelper.parseArguments(args)
    val itemDelimiter = " "

    //Parse input parameter
    val input = parameter.getOptionValue("input")
    val minSupport = parameter.getOptionValue("support")

    if (input == null || input == "" || minSupport == null) {
      println("Please indicate input file and support: --input inputFile --support minSupport")
      return
    }

    //This is a workout on windows to run spark locally. Set the hadoop.home.dir to your home hadoop folder
    val conf = new SparkConf().setAppName("SPARK PFPGrowth")
    val sc = new SparkContext(conf)

    val startTime: Long = System.currentTimeMillis()

    //SPARK RUNNING
    val transactionsSpark = sc.textFile(input).map(_.split(itemDelimiter)).cache()
    val modelSpark = new FPGrowth()
      .setMinSupport(minSupport.toDouble)
      .run(transactionsSpark)

    val frequentSet = modelSpark.freqItemsets.collect()

    println("TIME SPARK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
    println("SPARK FPGrowth: " + frequentSet.length)
    sc.stop()
  }
}
