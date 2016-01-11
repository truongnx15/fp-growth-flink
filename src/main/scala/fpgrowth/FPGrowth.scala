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
  var allFrequenSets = new ListBuffer[Itemset]()

  def this(itemsets: ListBuffer[Itemset], minCount: Long, order: immutable.HashMap[Item, Int], sorting: Boolean = true) = {
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
      itemsets.foreach{
        itemset => {itemset.items = itemset.items.filter(_.frequency >= minCount)}
      }
    }

    //Init fptree
    fptree = new FPTree(itemsets, minCount)
    fptree.buildFPTree()
  }

  /**
    * Update frequency of items in itemset
    */

  def updateFrequency(frequentMap: mutable.HashMap[Item, Long]): Unit = {
    itemsets.foreach {
      itemset => {
        val items = itemset.getItems
        items.foreach {
          item => {
            val frequency = frequentMap.getOrElse(item, 0L)
            item.frequency = frequency
          }
        }
      }
    }
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
            val frequency = tmpMap.getOrElseUpdate(item, 0) + item.count
            tmpMap += (item -> frequency)
          }
        }
      }
    }

    updateFrequency(tmpMap)

    //Build order
    val items = tmpMap.map( item => {
      new Item(item._1.name, item._2, item._1.count)
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
    var currentNode = fptree.headerTable(item)
    var itemCount: Long = 0;
    while (currentNode != null) {
      var itemset = new Itemset()
      itemCount += currentNode.frequency

      //Find the subpath to the root
      var pathNode = currentNode.parent
      while (!pathNode.isRoot) {
        itemset.addItem(new Item(pathNode.item.name, 1, currentNode.frequency))
        pathNode = pathNode.parent
      }

      //add the itemset to conditional pattern
      frequentItemsets += itemset

      //Move to next node
      currentNode = currentNode.nextNode
    }

    if (itemCount >= minCount) {
      return frequentItemsets
    }

    return new ListBuffer[Itemset]()
  }

  /**
    * Generate pattern if the tree has only one single path
    * @param fpTree
    * @return
    */

  def generateFrequentFromSinglePath(fpTree: FPTree): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()

    var path = new ListBuffer[Item]

    var currentNode: FPTreeNode = null
    fpTree.root.children.foreach {
      case (item, fpTreeNode) => {
        currentNode = fpTreeNode
      }
    }

    //Extract the path to a list
    while (currentNode != null) {
      path += new Item(currentNode.item.name, currentNode.frequency, currentNode.frequency)
      var nextNode: FPTreeNode = null
      currentNode.children.foreach {
        case (item, fpTreeNode) => {
          nextNode = fpTreeNode
        }
      }
      currentNode = nextNode
    }

    val numItemsets: Long = 1L << path.length

    for(i <- 1L to (numItemsets - 1)) {

      var currentItemset = new Itemset()
      var minItemSupport = Long.MaxValue

      for(j<- 0 to (path.size - 1)) {
        //bit j of i
        if (((i >> j) & 1) == 1) {
          // if bit j of i == 1 => get item j from path
          currentItemset.addItem(path(j))
          minItemSupport = math.min(minItemSupport, path(j).frequency)
        }
      }
      //Set support of the current itemset to the smallest of item
      currentItemset.support = minItemSupport

      //output current support
      frequentItemsets += currentItemset

    }

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
      if (fSets.size > 0) {
        frequentItemsets ++= fSets
      }

      fSets.foreach {
        set => {
          var newItemset = new Itemset()
          newItemset.items ++= itemset.items
          allFrequenSets += newItemset
        }
      }
    }
    else {
      fptree.headerTable.foreach {
        case (item, fpTreeNode) => {
          if (item.frequency >= minCount) {

            var currentItemset = new Itemset()
            currentItemset.addItem(item)
            if (itemset != null) {
              currentItemset.items ++= itemset.items
            }

            frequentItemsets += currentItemset
            allFrequenSets += currentItemset

            val conditionalPatterns = generateConditionalBasePatterns(fptree, item)
            if (conditionalPatterns.size > 0) {
              val tmpFPGrowth = new FPGrowth(conditionalPatterns, minCount, true)
              var fSets = tmpFPGrowth.extractPattern(tmpFPGrowth.fptree, currentItemset)
              frequentItemsets ++= fSets
              allFrequenSets ++= fSets
            }
          }
        }
      }
    }

    frequentItemsets.foreach {
      set => {
        if (itemset != null) set.items ++= itemset.items
      }
    }
    return frequentItemsets
  }
}
