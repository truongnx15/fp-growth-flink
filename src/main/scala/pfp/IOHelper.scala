package pfp

import fpgrowth.Item
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object IOHelper {
  def readInput(env: ExecutionEnvironment, input: String, itemDelimiter: String): DataSet[ArrayBuffer[Item]] = {
    //Read dataset
    env.readTextFile(input)
      .flatMap(new FlatMapFunction[String, ArrayBuffer[Item]] {
        override def flatMap(line: String, out: Collector[ArrayBuffer[Item]]): Unit = {
          val itemset = ArrayBuffer.empty[Item]
          val items = line.split(itemDelimiter)

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
