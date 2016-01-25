package pfp

import fpgrowth.{Item, Itemset}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

object IOHelper {
  def readInput(env: ExecutionEnvironment, input: String, itemDelimiter: String): DataSet[Itemset[Item]] = {
    //Read dataset
    env.readTextFile(input)
      .flatMap(new FlatMapFunction[String, Itemset[Item]] {
        override def flatMap(line: String, out: Collector[Itemset[Item]]): Unit = {
          val itemset = new Itemset[Item]()
          val items = line.split(itemDelimiter)

          if (items.nonEmpty) {
            items.foreach { x =>
              if (x.length() > 0) itemset.addItem(new Item(x, 0))
            }
            out.collect(itemset)
          }
        }
      })
  }
}
