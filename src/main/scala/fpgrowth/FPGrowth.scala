//package fpgrowth
package fpgrowth

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{immutable, mutable}


/**
  * FPGrowth in memory
  *
  * @param itemsets the input transactions
  * @param minCount minimum occurrences to be considered
  * @param sorting whether the itemset should be sorted based on order of frequency
  */
class FPGrowth(var itemsets: ListBuffer[ArrayBuffer[Item]], var minCount: Long, var sorting: Boolean) {

  var fptree: FPTree = new FPTree(minCount)

  /*
  if (sorting) {
    //Build the order

  }

  if (itemsets != null && itemsets.nonEmpty) {
    //Add each of the transaction to the tree
    itemsets.foreach {
      itemset => {
        fptree.addTransaction(itemset, 1)
      }
    }
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
  */

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
    val listNode = fptree.headerTable(itemId)

    listNode.foreach(
      currentNode => {
        var pathNode = currentNode.parent
        var itemset = List.empty[Int]

        while (!pathNode.isRoot) {
          itemset = pathNode.itemId :: itemset
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

  def generateFrequentFromSinglePath(fpTree: FPTree, inputItemId: Int = Int.MaxValue): ListBuffer[(ArrayBuffer[Int], Int)] = {
    var frequentItemsets = new ListBuffer[(ArrayBuffer[Int], Int)]()

    var path = new ListBuffer[FPTreeNode]

    var localInputItemId: Int = Int.MaxValue
    var localInputItemIdFrequency = Int.MaxValue

    var currentNode: FPTreeNode = null
    fpTree.root.children.foreach {
      case (item: Int, fpTreeNode: FPTreeNode) => currentNode = fpTreeNode
    }

    //Extract the path to a list
    while (currentNode != null && currentNode.frequency >= minCount && localInputItemId == Int.MaxValue) {

      if (inputItemId != Int.MaxValue && currentNode.itemId.equals(inputItemId)) {
        localInputItemId = currentNode.itemId
        localInputItemIdFrequency = currentNode.frequency
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
    if (inputItemId != Int.MaxValue && localInputItemId == Int.MaxValue) {
      return frequentItemsets
    }

    val numItemsets: Long = 1L << path.length

    //1 means choosing at least one element in the list
    var startSubset = 1L
    if (localInputItemId != Int.MaxValue) {
      //in this case, it is possible to have empty subset because localInputItemId will be added later
      startSubset = 0L
    }

    for(i <- startSubset to (numItemsets - 1)) {

      var currentItemset = ArrayBuffer.empty[Int]
      var minItemSupport = Int.MaxValue

      for(j<- path.indices) {
        //bit j of i
        if (((i >> j) & 1) == 1) {
          // if bit j of i == 1 => get item j from path
          currentItemset += path(j).itemId
          minItemSupport = math.min(minItemSupport, path(j).frequency)
        }
      }

      if (inputItemId != Int.MaxValue) {
        minItemSupport = math.min(minItemSupport, localInputItemIdFrequency)
        currentItemset += localInputItemId
      }

      //output current support
      frequentItemsets += ((currentItemset, minItemSupport))
    }

    //Return frequent itemset
    frequentItemsets
  }

  /**
    * Extract pattern from FPTree
    *
    * @param fpTree The FPTree from which frequent patterns are extracted
    * @param itemset The current suffix of FPTree from which conditional base patterns are extracted
    * @param inputItem If given, only extract frequent patterns for the suffix starting inputItem
    * @return List of (Itemset, Support) as frequent itemsets
    */
  def extractPattern(fpTree: FPTree, itemset: ArrayBuffer[Int], inputItem: Int = Int.MaxValue): ListBuffer[(ArrayBuffer[Int], Int)] = {
    var frequentItemsets = new ListBuffer[(ArrayBuffer[Int], Int)]()
    if (this.fptree.hasSinglePath) {
      var fSets = generateFrequentFromSinglePath(fpTree, inputItem)
      if (fSets.nonEmpty) {
        if (itemset != null) {
          fSets.foreach {
            case (fItemset, support) => frequentItemsets += ((itemset ++ fItemset, support))
          }
        }
        //frequentItemsets ++= fSets
      }
    }
    else {
      fptree.headerTable.foreach {
        case (itemId, listFPTreeNode) => {

          if ((inputItem == Int.MaxValue || itemId.equals(inputItem))) {

            val itemFrequency = fptree.itemFrequencyTable(itemId)

            if (itemFrequency >= minCount) {
              var currentItemset = ArrayBuffer.empty[Int]

              currentItemset += itemId

              if (itemset != null) {
                currentItemset = itemset ++ currentItemset
              }

              frequentItemsets += ((currentItemset, itemFrequency))

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
}
