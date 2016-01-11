package pfp

import java.lang.Iterable

import fpgrowth.{Item, Itemset}
import org.apache.flink.api.common.functions.{GroupReduceFunction, FlatMapFunction}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable


object Aggregation {

  /**
    * Map function for Aggregation algorithm of Parallel FPGrowth
    * @return
    */
  def AggregationFlatMap = new FlatMapFunction[Itemset, (Item, Itemset)] {
    override def flatMap(item: Itemset, collector: Collector[(Item, Itemset)]): Unit = {
      val itemset = item.getItems
      itemset.foreach { x => collector.collect((x, item))}
    }
  }

  def AggregationGroupReduce = new GroupReduceFunction[(Item, Itemset), Itemset] {
    override def reduce(iterable: Iterable[(Item, Itemset)], collector: Collector[Itemset]): Unit = {
      iterable.foreach(
        tuple => {
          collector.collect(tuple._2)
        }
      )
    }
  }
}
