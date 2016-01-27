package pfp

import fpgrowth.{FPGrowth => FPGrowthLocal, Item}
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * Created by Xuan Truong on 25-Jan-16.
  */
object LocalFPGrowth {
  def main(args: Array[String]): Unit = {
    //Employ flink and FPGrowth to read data
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val transactions = IOHelper.readInput(env, "T40I10D100K.dat", " ")
    val minCount: Long = math.ceil(5.0/100 * transactions.count()).toLong

    val starTime = System.currentTimeMillis()

    //Init and run FPGrowth
    val sorting: Boolean = true
    val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(transactions.collect().toList, minCount, sorting)

    //Result result
    val result = fpGrowthLocal.getFrequentItemsets()
    println("LOCAL FPGROWTH: " + result.size)
    println("TIME: " + (System.currentTimeMillis() - starTime) / 1000.0)
    //result.foreach(println(_))
  }
}
