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

  def this(itemsets: ListBuffer[Itemset], minCount: Long, order: immutable.Map[Item, Int], sorting: Boolean = true) = {
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

    this.order = items.sortWith(_.frequency > _.frequency).zipWithIndex.toMap
  }

  /**
    * Generate the conditional based patterns for one item in the header table of fpTree
    * @param fpTree
    * @param item
    * @return
    */

  def generateConditionalBasePatterns(fpTree: FPTree, item: Item): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()
    var frequencyMap = mutable.HashMap.empty[Item, Long]

    //Adjust frequent of item in conditional pattern to be as the same as item
    var currentNode = fptree.headerTable(item)
    var itemCount: Long = 0;
    while (currentNode != null) {
      var itemset = new Itemset()
      itemCount += currentNode.frequency

      //Find the subpath to the root
      var pathNode = currentNode.parent
      while (!pathNode.isRoot) {
        val newItem = new Item(pathNode.item.name, 1, currentNode.frequency)
        itemset.addItem(newItem)

        val countFrequency = frequencyMap.getOrElseUpdate(newItem, 0) + currentNode.frequency
        frequencyMap += (newItem -> countFrequency)

        pathNode = pathNode.parent
      }

      //add the itemset to conditional pattern
      if (itemset.items.size > 0) {
        frequentItemsets += itemset
      }

      //Move to next node
      currentNode = currentNode.nextNode
    }

    if (itemCount >= minCount) {

      frequentItemsets.foreach {
        itemset => {
          val items = itemset.getItems
          items.foreach {
            item => {
              val frequency = frequencyMap.getOrElse(item, 0L)
              item.frequency = frequency
            }
          }
        }
      }


      frequentItemsets.foreach{
        itemset => {itemset.items = itemset.items.filter(_.frequency >= minCount)}
      }


      //Build order
      val items = frequencyMap.map( item => {
        new Item(item._1.name, item._2, item._1.count)
      }).toList

      val localOrder = items.sortWith(_.frequency > _.frequency).zipWithIndex.toMap

      //Reorder item in transaction based on the order
      frequentItemsets.foreach(_.sortItems(localOrder))

      return frequentItemsets
    }

    return new ListBuffer[Itemset]()
  }

  /**
    * Generate pattern if the tree has only one single path
    * @param fpTree
    * @return
    */

  def generateFrequentFromSinglePath(fpTree: FPTree, inputItem: Item = null): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()

    var path = new ListBuffer[Item]

    var localInputItem:Item = null

    var currentNode: FPTreeNode = null
    fpTree.root.children.foreach {
      case (item, fpTreeNode) => {
        currentNode = fpTreeNode
      }
    }

    //Extract the path to a list
    while (currentNode != null) {
      if (inputItem != null && currentNode.item.equals(inputItem)) {
        localInputItem = currentNode.item
      }
      else {
        path += new Item(currentNode.item.name, currentNode.frequency, currentNode.frequency)
      }
      var nextNode: FPTreeNode = null
      currentNode.children.foreach {
        case (item, fpTreeNode) => {
          nextNode = fpTreeNode
        }
      }
      currentNode = nextNode
    }

    //Not found any itemset that contains inputItem
    if (inputItem != null && localInputItem == null) {
      return frequentItemsets
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

      if (inputItem != null) {
        currentItemset.support = math.min(currentItemset.support, localInputItem.frequency)
        currentItemset.addItem(localInputItem)
      }

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
  def extractPattern(fpTree: FPTree, itemset: Itemset, inputItem: Item = null): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()
    if (this.fptree.hasSinglePath && inputItem == null) {
      var fSets = generateFrequentFromSinglePath(fpTree)
      if (fSets.size > 0) {
        frequentItemsets ++= fSets
      }
    }
    else {
      fptree.headerTable.foreach {
        case (item, fpTreeNode) => {
          var itemFrequency: Long = 0;
          var tmpFPTreeNode = fpTreeNode
          while (tmpFPTreeNode != null) {
            itemFrequency += tmpFPTreeNode.frequency
            tmpFPTreeNode = tmpFPTreeNode.nextNode
          }
          if (itemFrequency >= minCount && (inputItem == null || item.equals(inputItem))) {

            var currentItemset = new Itemset()
            currentItemset.addItem(item)
            if (itemset != null) {
              currentItemset.items ++= itemset.items
              currentItemset.setSupport(item.frequency)
            }

            frequentItemsets += currentItemset

            val conditionalPatterns = generateConditionalBasePatterns(fptree, item)
            if (conditionalPatterns.size > 0) {
              val tmpFPGrowth = new FPGrowth(conditionalPatterns, minCount, false)
              var fSets: ListBuffer[Itemset] = tmpFPGrowth.extractPattern(tmpFPGrowth.fptree, currentItemset)
              if (fSets.size > 0) {
                frequentItemsets ++= fSets
              }
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
