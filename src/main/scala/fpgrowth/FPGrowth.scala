//package fpgrowth
package fpgrowth

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}


/**
  * FPGrowth in memory
  * @param itemsets the input transactions
  * @param minCount minimum occurrences to be considerd
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
        val items = itemset.items
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
        val items = itemset.items
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
    * @param fpTree the tree from which conditional base patterns are extracted
    * @param item the item for which the conditional base patterns are extracted
    * @return list of conditional base patterns
    */

  def generateConditionalBasePatterns(fpTree: FPTree, item: Item): ListBuffer[Itemset] = {
    var conditionalItemsets = new ListBuffer[Itemset]()
    //var frequencyMap = mutable.HashMap.empty[Item, Long]

    //Adjust frequent of item in conditional pattern to be as the same as item
    val listNode = fptree.headerTable(item)

    listNode.foreach(
      f = currentNode => {
        var pathNode = currentNode.parent
        var itemset = new Itemset()

        while (!pathNode.isRoot) {

          val newItem = new Item(pathNode.item.name, 1, currentNode.frequency)
          itemset.addItem(newItem)

          //val countFrequency = frequencyMap.getOrElseUpdate(newItem, 0) + (currentNode.frequency)
          //frequencyMap += (newItem -> countFrequency)

          pathNode = pathNode.parent
        }

        if (itemset.items.nonEmpty) {
          itemset.items = itemset.items.reverse
          conditionalItemsets += itemset
        }
      }
    )

    //Return conditional patterns
    conditionalItemsets
  }

  /**
    * Generate pattern if the tree has only one single path
    * @param fpTree: The single path tree in which frequent itemsets are extracted
    * @return List of frequent itemset
    */

  def generateFrequentFromSinglePath(fpTree: FPTree, inputItem: Item = null): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()

    var path = new ListBuffer[Item]

    var localInputItem:Item = null

    var currentNode: FPTreeNode = null
    fpTree.root.children.foreach {
      case (item: Item, fpTreeNode: FPTreeNode) => currentNode = fpTreeNode
    }

    //Extract the path to a list
    while (currentNode != null && currentNode.frequency >= minCount) {

      if (inputItem != null && currentNode.item.equals(inputItem)) {
        localInputItem = currentNode.item
      }
      else {
        path += new Item(currentNode.item.name, currentNode.frequency, currentNode.frequency)
      }
      var nextNode: FPTreeNode = null
      currentNode.children.foreach {
        case (item, fpTreeNode) => nextNode = fpTreeNode
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

      for(j<- path.indices) {
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

    //Return frequent itemset
    frequentItemsets
  }

  /**
    * Extract pattern from FPTree
    * @param fpTree The FPTree from which frequent patterns are extracted
    * @param itemset The current suffix of FPTree from which conditional base patterns are extracted
    * @param inputItem If given, only extract frequent patterns for the suffix starting inputItem
    * @return List of Itemsets as frequent itemsets
    */
  def extractPattern(fpTree: FPTree, itemset: Itemset, inputItem: Item = null): ListBuffer[Itemset] = {
    var frequentItemsets = new ListBuffer[Itemset]()
    if (this.fptree.hasSinglePath) {
      var fSets = generateFrequentFromSinglePath(fpTree, inputItem)
      if (fSets.nonEmpty) {
        fSets.foreach {fItemset => fItemset.items = itemset.items ++ fItemset.items}
        frequentItemsets ++= fSets
      }
    }
    else {
      fptree.headerTable.foreach {

        case (item, listFPTreeNode) => {
          var itemFrequency: Long = 0
          listFPTreeNode.foreach(itemFrequency += _.frequency)
          item.frequency = itemFrequency

          if (item.frequency >= minCount && (inputItem == null || item.equals(inputItem))) {
            var currentItemset = new Itemset()
            currentItemset.addItem(item)
            currentItemset.support = item.frequency
            if (itemset != null) {
              currentItemset.items = itemset.items ++ currentItemset.items
            }

            frequentItemsets += currentItemset

            val conditionalPatterns = generateConditionalBasePatterns(fptree, item)
            if (conditionalPatterns.nonEmpty) {
              val tmpFPGrowth = new FPGrowth(conditionalPatterns, minCount, false)
              var fSets: ListBuffer[Itemset] = tmpFPGrowth.extractPattern(tmpFPGrowth.fptree, currentItemset)
              if (fSets.nonEmpty) {
                frequentItemsets ++= fSets
              }
            }
          }
        }
      }
    }

    //Return the result
    frequentItemsets
  }
}
