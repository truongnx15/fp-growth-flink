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
    val transactions = IOHelper.readInput(env, "sample_fpgrowth_local_full.txt", " ")
    val minCount: Long = math.ceil(3.0/5 * transactions.count()).toLong

    //convert DataSet to ListBuffer
    var inputTransactions =  ListBuffer.empty[ListBuffer[Item]]
    transactions.collect().flatMap(inputTransactions += _)

    //Init and run FPGrowth
    val sorting: Boolean = true
    val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(inputTransactions, minCount, sorting)

    //Result result
    val result = fpGrowthLocal.getFrequentItemsets()
    println("LOCAL FPGROWTH: " + result.size)
    //result.foreach(println(_))
  }
}
