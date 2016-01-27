package pfp

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Xuan Truong on 25-Jan-16.
  */
object SparkFPGrowth {
  def main(args: Array[String]): Unit = {
    //This is a workout on windows to run spark locally. Set the hadoop.home.dir to your home hadoop folder
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common")
    val conf = new SparkConf().setAppName("PFPGrowth").setMaster("local[*]")
    val sc = new SparkContext(conf)

    var startTime: Long = System.currentTimeMillis()

    //SPARK RUNNING
    val transactionsSpark = sc.textFile("T40I10D100K.dat").map(_.split(" ")).cache()
    val modelSpark = new FPGrowth()
      .setMinSupport(5.0/100)
      .run(transactionsSpark)

    val frequentSet = modelSpark.freqItemsets.collect()

    println("SPARK FPGrowth: " + frequentSet.size)
    println("TIME SPARK: " + (System.currentTimeMillis() - startTime)/1000.0 + "\n")
    startTime = System.currentTimeMillis()
    sc.stop()
  }
}
