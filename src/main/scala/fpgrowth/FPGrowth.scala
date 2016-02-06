//package fpgrowth
package fpgrowth

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * FPGrowth in memory
  *
  * @param itemsets the input transactions
  * @param minCount minimum occurrences to be considered
  * @param sorting whether the itemset should be sorted based on order of frequency
  */
class FPGrowth(var itemsets: List[ListBuffer[Item]], var minCount: Long, var sorting: Boolean) {

  //This is for local FPGrowth if someone wants to run FPGrowth in local
  var order: Map[Item, Int] = null
  //Transactions are transformed to transactions of itemId(order of item in the original transaction if items are sorted in increasing
  //order of frequency
  var itemsetsId: List[ListBuffer[Int]] = null
  //Map between itemId(order of item) and item itself
  var itemIdItemMap: Map[Int, Item] = null

  var fptree: FPTree = new FPTree(minCount)

  if (sorting) {
    //Build the order
    if (order == null) {
      buildItemOrder()
    }

    itemsetsId = itemsets.map(_.flatMap(order.get).sorted)
    itemIdItemMap = order.map(_.swap)
  }

  if (itemsetsId != null && itemsetsId.nonEmpty) {
    //Add each of the transaction to the tree
    itemsetsId.foreach {
      itemset => {
        fptree.addTransaction(itemset, 1)
      }
    }
  }

  /**
    * Update frequency of items in itemset
    */

  def updateFrequency(frequentMap: mutable.HashMap[Item, Int]): Unit = {
    itemsets.foreach {
      itemset => {
        itemset.foreach {
          item => {
            val frequency = frequentMap.getOrElse(item, 0)
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
    val tmpMap = mutable.HashMap.empty[Item, Int]
    itemsets.foreach {
      itemset => {
        itemset.foreach {
          item => {
            val frequency = tmpMap.getOrElseUpdate(item, 0) + 1
            tmpMap += (item -> frequency)
          }
        }
      }
    }

    //Update frequency of items
    updateFrequency(tmpMap)

    //Build order
    val items = tmpMap.map( item => {
      new Item(item._1.name, item._2)
    }).toList.filter(_.frequency >= minCount)

    //filter non-frequent item
    itemsets = itemsets.map(_.filter(_.frequency >= minCount))

    this.order = items.sortWith(_.frequency > _.frequency).zipWithIndex.toMap
  }

  /**
    * Generate the conditional based patterns for one item in the header table of fpTree
    *
    * @param fpTree the tree from which conditional base patterns are extracted
    * @param itemId the itemId for which the conditional base patterns are extracted
    * @return list of conditional base patterns
    */

  def generateConditionalBasePatterns(fpTree: FPTree, itemId: Int): FPGrowth = {

    val tmpFPGrowth = new FPGrowth(null, minCount, false)

    //Adjust frequent of item in conditional pattern to be as the same as item
    val headerTableItem = fptree.headerTable(itemId)

    var itemset = ListBuffer.empty[Int]

    headerTableItem.nodes.foreach(
      currentNode => {
        var pathNode = currentNode.parent

        //var itemset = List.empty[Int]
        itemset.clear()

        while (!pathNode.isRoot) {
          //itemset = pathNode.itemId :: itemset
          itemset.prepend(pathNode.itemId)
          pathNode = pathNode.parent
        }

        if (itemset.nonEmpty) {
          tmpFPGrowth.fptree.addTransaction(itemset, currentNode.frequency)
        }
      }
    )

    //Return conditional patterns
    //conditionalItemsets
    tmpFPGrowth
  }

  /**
    * Generate pattern if the tree has only one single path
    *
    * @param fpTree: The single path tree in which frequent itemsets are extracted
    * @return List of frequent itemset
    */

  def generateFrequentFromSinglePath(fpTree: FPTree, inputItemId: Int = Int.MinValue): ListBuffer[(ListBuffer[Int], Int)] = {
    var frequentItemsets = new ListBuffer[(ListBuffer[Int], Int)]()

    var path = new ListBuffer[FPTreeNode]

    var localInputItem: FPTreeNode = null

    var currentNode: FPTreeNode = null
    fpTree.root.children.foreach {
      case (item: Int, fpTreeNode: FPTreeNode) => currentNode = fpTreeNode
    }

    //Extract the path to a list
    while (currentNode != null && currentNode.frequency >= minCount && localInputItem == null) {

      if (inputItemId != Int.MinValue && currentNode.itemId.equals(inputItemId)) {
        localInputItem = currentNode
      }
      else {
        path += currentNode
      }
      var nextNode: FPTreeNode = null
      currentNode.children.foreach {
        case (item, fpTreeNode) => nextNode = fpTreeNode
      }
      currentNode = nextNode
    }

    //Not found any itemset that contains inputItem
    /*
    if (inputItemId != Int.MinValue && localInputItem == null) {
      return frequentItemsets
    }
    */

    val numItemsets: Long = 1L << path.length

    //1 means choosing at least one element in the list
    var startSubset = 1L
    if (localInputItem != null) {
      //in this case, it is possible to have empty subset because localInputItemId will be added later
      startSubset = 0L
    }

    for(i <- startSubset to (numItemsets - 1)) {

      var currentItemset = ListBuffer.empty[Int]
      var minItemSupport = Int.MaxValue

      for(j<- path.indices) {
        //bit j of i
        if (((i >> j) & 1) == 1) {
          // if bit j of i == 1 => get item j from path
          currentItemset += path(j).itemId
          minItemSupport = math.min(minItemSupport, path(j).frequency)
        }
      }

      if (inputItemId != Int.MinValue) {
        minItemSupport = math.min(minItemSupport, localInputItem.frequency)
        currentItemset += localInputItem.itemId
      }

      //output current support
      frequentItemsets += ((currentItemset, minItemSupport))
    }

    //Return frequent itemset
    frequentItemsets
  }

  /**
    * Extract pattern from FPTree. The frequent itemsets are the frequent ItemIdset
    *
    * @param fpTree The FPTree from which frequent patterns are extracted
    * @param itemset The current suffix of FPTree from which conditional base patterns are extracted
    * @param inputItem If given, only extract frequent patterns for the suffix starting inputItem
    * @return List of (ItemIdset, Support) as frequent itemsets
    */
  def extractPattern(fpTree: FPTree, itemset: ListBuffer[Int], inputItem: Int = Int.MinValue): ListBuffer[(ListBuffer[Int], Int)] = {
    var frequentItemsets = new ListBuffer[(ListBuffer[Int], Int)]()
    if (this.fptree.hasSinglePath) {
      val fSets = generateFrequentFromSinglePath(fpTree, inputItem)
      if (fSets.nonEmpty) {
        if (itemset != null) {
          fSets.foreach {
            case (fItemset, support) => frequentItemsets += ((itemset ++ fItemset, support))
          }
        }
        else {
          frequentItemsets ++= fSets
        }
      }
    }
    else {
      fptree.headerTable.foreach {
        case (itemId, headerTableItem) => {

          if (inputItem == Int.MinValue || itemId.equals(inputItem)) {

            if (headerTableItem.count >= minCount) {
              var currentItemset = ListBuffer.empty[Int]

              currentItemset += itemId

              if (itemset != null) {
                currentItemset ++= itemset
              }

              frequentItemsets += ((currentItemset, headerTableItem.count))

              val tmpFPGrowth = generateConditionalBasePatterns(fptree, itemId)
              if (tmpFPGrowth.fptree.headerTable.nonEmpty) {
                var fSets = tmpFPGrowth.extractPattern(tmpFPGrowth.fptree, currentItemset)
                if (fSets.nonEmpty) {
                  frequentItemsets ++= fSets
                }
              }
            }
          }
        }
      }
    }

    //Return the result
    frequentItemsets
  }

  /**
    * Get the frequent itemset
    *
    * @return the list of (itemset, support)
    */

  def getFrequentItemsets(): ListBuffer[(ListBuffer[Item], Int)] = {
    var frequentItemsets = new ListBuffer[(ListBuffer[Item], Int)]()

    val frequentItemIdSet = extractPattern(this.fptree, null)
    frequentItemIdSet.foreach(
      frequentIdSet => {
        val itemset = frequentIdSet._1.flatMap(itemIdItemMap.get)
        frequentItemsets += ((itemset, frequentIdSet._2))
      }
    )
    //Return result
    frequentItemsets
  }
}
