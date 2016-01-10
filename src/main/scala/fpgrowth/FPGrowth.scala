//package fpgrowth
package fpgrowth

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}


/**
  * FPGrowth in memory
  * @param itemsets
  * @param minCount
  * @param sorting whether the itemset should be sorted based on order of frequency
  */
class FPGrowth(var itemsets: ListBuffer[Itemset], var minCount: Long, var sorting: Boolean) {

  var order: immutable.Map[Item, Int] = null

  def this(itemsets: ListBuffer[Itemset], minCount: Long, sorting: Boolean = true, order: immutable.HashMap[Item, Int]) {
    this(itemsets, minCount, sorting)
    this.order = order
  }

  var fptree: FPTree = _

  {
    if (sorting) {
      if (order == null) {
        //Build the order of item from highest frequency to lowest
        buildItemOrder()
      }
      //Reorder item in transaction based on the order
      itemsets.foreach(_.sortItems(order))
    }

    //Init fptree
    fptree = new FPTree(itemsets, minCount)
    fptree.buildFPTree()
  }

  /**
    * We need to build the order of item for the tree
    */
  def buildItemOrder(): Unit = {
    //Build the order to sort item
    val tmpMap = mutable.HashMap.empty[Item, Long]
    itemsets.foreach {
      itemset => {
        val items = itemset.getItems
        items.foreach {
          item => {
            val frequency = tmpMap.getOrElseUpdate(item, 0L)
            tmpMap += item -> frequency
          }
        }
      }
    }

    //Build order
    val items = tmpMap.map( item => {
      new Item(item._1.name, item._2)
    }).toList

    this.order = items.sortWith(_ > _).zipWithIndex.toMap
  }

  /**
    * Generate the conditional based patterns for one item in the header table of fpTree
    * @param fpTree
    * @param item
    * @return
    */

  def generateConditionalBasePatterns(fpTree: FPTree, item: Item): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()

    //Adjust frequent of item in conditional pattern to be as the same as item

    return frequentItemsets
  }

  /**
    * Generate pattern if the tree has only one single path
    * @param fpTree
    * @return
    */

  def generateFrequentFromSinglePath(fpTree: FPTree): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()

    return frequentItemsets
  }

  /**
    * Extract pattern from FPTree
    * @param fpTree
    * @param itemset
    * @return
    */
  def extractPattern(fpTree: FPTree, itemset: Itemset): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()
    if (this.fptree.hasSinglePath) {
      var fSets = generateFrequentFromSinglePath(fpTree)
      fSets.foreach { set => set.items.++=(itemset.items)}
      frequentItemsets.++=(fSets)
    }
    else {
      fptree.headerTable.foreach {
        case (item, fpTreeNode) => {
          val conditionalPatterns = generateConditionalBasePatterns(fptree, item)
          //Init the tree
          val tempFPTree = new FPTree(conditionalPatterns, minCount)
          //Build the tree
          tempFPTree.buildFPTree()

          //Extract frequent pattern
          itemset.items += item
          val fSets = extractPattern(tempFPTree, itemset)

          itemset.items = itemset.items.dropRight(1)
        }
      }
    }

    frequentItemsets.foreach {_.items.++=(itemset.items)}
    return frequentItemsets
  }
}
