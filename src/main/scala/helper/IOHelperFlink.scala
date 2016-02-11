package helper

import fpgrowth.Item
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * This is a helper to read transaction from input file for flink
  */

object IOHelperFlink {
  /**
    * Read transactions in text file for Flink
    *
    * @param env The Flink runtime environment
    * @param input The path to input file
    * @param itemDelimiter The delimiter of items within one transaction
    * @return DataSet of transactions(ListBuffer[Item])
    */
  def readInput(env: ExecutionEnvironment, input: String, itemDelimiter: String): DataSet[ListBuffer[Item]] = {
    //Read dataset
    env.readTextFile(input)
      .flatMap(new FlatMapFunction[String, ListBuffer[Item]] {
        override def flatMap(line: String, out: Collector[ListBuffer[Item]]): Unit = {

          val itemset = ListBuffer.empty[Item]
          //Split line to get items
          val items = line.trim.split(itemDelimiter)

          if (items.nonEmpty) {
            items.foreach { x =>
              if (x.length() > 0) itemset += new Item(x, 0)
            }
            out.collect(itemset)
          }
        }
      })
  }
}
