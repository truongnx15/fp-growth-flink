package pfp

import java.{lang, util}

import fpgrowth.{Item, Itemset}
import org.apache.flink.api.common.functions.{RichGroupReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable._

//Object to make the ParallelFPGrowth static
object ParallelFPGrowth {

  /**
    * Map function for Step 4: Generate group-dependent transactions
    * Input: Itemset
    * Output key: group-id
    * Output value: generated group-dependent transactions
    */
  def ParallelFPGrowthRichMap = new RichMapFunction[Itemset, (Long, Itemset)] {
    //Load G-List
    var gList: List[(Item, Long)] = null

    override def open(config: Configuration): Unit = {
      gList = getRuntimeContext().getBroadcastVariable[(Item, Long)]("gList").asScala.toList
    }

    // Generate Hash Table H from G-List
    val hTable = HashMap.empty[Item,Long]

    gList.foreach {
      case (item, gid) => {
        hTable += (item -> gid)
      }
    }

    // a[] <- Split(Ti)

    override def map(transaction: Itemset): (Long, Itemset) = {
      val itemset = transaction.getItems()
      var hashNumber: Long = 0
      val itemset2:Itemset = null

      transaction.items.foreach {
        x => {
          hashNumber = hTable(x)
          if(hashNumber != null) {
            // Delete all pairs which hash value is HashNum in H
            //TODO
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
    //def reduce(items: Iterable[(Long, Itemset)], out: Collector[(Itemset, Long)]): Unit = {
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

    override def reduce(iterable: lang.Iterable[(Long, Itemset)], collector: Collector[(Itemset, Long)]): Unit = ???
  }

}
