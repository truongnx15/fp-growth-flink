package pfp

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

import fpgrowth.Item
import fpgrowth.Itemset

//Object to make the ParallelCounting static
object ParalellCounting {
  
  /**
   * Map function for Step 2: Parallel Counting in PFP
   * For each item in every transaction, output a pair (item, occurrence[default 1])
   * These pairs are latter grouped to count occurrence for each distinct item
   */
  
  def ParalellCountingFlatMap = new FlatMapFunction[Itemset, Item] {
    override def flatMap(transaction: Itemset, out: Collector[Item]): Unit = {
      //Retrieve the list of item in a transaction
      var itemset = transaction.getItems();
      
      //For each item in the transaction, output pair (item, occurrence[1 by default])
      itemset.map { 
        x => {
          x.frequency = 1
          out.collect(x)
        }
      }
    }
  }
  
  def ParalellCountingGroupReduce = new GroupReduceFunction[Item, Item] {
    override def reduce(items: java.lang.Iterable[Item], out: Collector[Item]): Unit = {
      
      //Temporary variable before returning the final result
      var sum: Long = 0
      var item: Item = null
      
      //Loop through the group and sum number of occurrences for the item
      items.map { 
        x => {
          item = x
          sum += x.frequency
        }
      }
      
      //Return the item with its frequency
      if (item != null) {
        item.frequency = sum
        out.collect(item)
      }
    }
  }
}