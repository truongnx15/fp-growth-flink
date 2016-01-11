package pfp

import java.util

import fpgrowth.{Item, Itemset}
import org.apache.flink.api.common.functions.{RichGroupReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.mutable._
import scala.collection.JavaConverters._

//Object to make the ParallelFPGrowth static
class ParallelFPGrowth {

  /**
    * Map function for Step 4: Generate group-dependent transactions
    * Input: Itemset
    * Output key: group-id
    * Output value: generated group-dependent transactions
    */
  def ParallelFPGrowthRichMap = new RichMapFunction[Itemset, (Item, Long)] {
    //Load G-List
    var gList: util.List[(Item, Long)] = null

    override def open(config: Configuration): Unit = {
      gList = getRuntimeContext().getBroadcastVariable[(Item, Long)]("gList")
    }

    // Generate Hash Table H from G-List
    val hTable = HashMap.empty[Item,Long]
    var a = 0
    for(a <- 0 to gList.size()-1) {
      hTable.put(gList.get(a)._1, gList.get(a)._1.hashCode)
    }

    // a[] <- Split(Ti)

    def map(transaction: Itemset): (Long, Itemset) = {
      val itemset = transaction.getItems()
      var hashNumber:Long = 0
      val itemset2:Itemset = null

      itemset.foreach {
        x => {
          hashNumber = hTable.get(x).hashCode()
          if(hashNumber != null) {
            // Delete all pairs which hash value is HashNum in H
            hTable.remove(x)
            itemset2.addItem(x)
          }
        }
      }
      (hashNumber, itemset2)
    }
  }

  /**
    * Reduce function for Step 4: FP-Growth on group-dependent shards
    * Groups all corresponding group-dependent transactions into a shard of group-dependent transactions.
    * Involves building a local FP-tree and growth its conditional FP-trees recursively
    * Input: key = group id, value = DBgid
    * Output: vi+support(vi)
    */
  def ParallelFPGrowthRichGroupReduce = new RichGroupReduceFunction[(Long, Itemset), (Itemset,Long)] {
    // Load G-List
    var gList: util.List[(Item, Long)] = null
    var nowGroup:util.List[(Item, Long)] = null
    //var localFPTree: LocalFPTree = null

    override def open(config: Configuration): Unit = {
      gList = getRuntimeContext().getBroadcastVariable[(Item, Long)]("gList")
    }

    var scalaGList = gList.asScala.toList
    def reduce(items: Iterable[(Long, Itemset)], out: Collector[(Itemset, Long)]): Unit = {
      items.foreach {
        item => {
          nowGroup.clear()
          // clear local FPTree
          scalaGList.foreach {
            x => {
              if(x._2.equals(item._1)) {
                nowGroup.add(x)
              }
            }
          }
          // Call insert-build-fp-tree(LocalFPtree,Ti);
        }
      }

      nowGroup.asScala.toList.foreach {
        y => {
          // Define and clear a size K max heap : HP;
          val k = 100
          val HPlist = new ListBuffer[Itemset]
          //TODO HPList = TopKFPGrowth(LocalFPtree,ai,HP);
          HPlist.foreach {
            v => {
              out.collect(v, v.getSupport())
            }
          }
        }
      }
    }
  }

}
