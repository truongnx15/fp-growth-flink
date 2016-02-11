package fpgrowth

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * FPGrowth in memory
  *
  * @param itemsets the input transactions
  * @param minCount minimum occurrences to be considered.
  * @param sorting whether the itemset should be sorted based on order of frequency.
  */
class FPGrowth(var itemsets: List[ListBuffer[Item]], var minCount: Long, var sorting: Boolean) {

  //This is for local FPGrowth if someone wants to run FPGrowth in local
  var order: Map[Item, Int] = null

  //Transactions are transformed to transactions of itemId(order of item in the original transaction if items are sorted in increasing
  //order of frequency
  var itemsetsId: List[ListBuffer[Int]] = null

  //Map between itemId(order of item) and item itself
  var itemIdItemMap: Map[Int, Item] = null

  var fptree: FPTree = new FPTree()

  if (sorting) {
    //Build the order
    if (order == null) {
      buildItemOrder()
    }

    //Convert transactions from list of Item to list of integer(position in sorted list of items)
    itemsetsId = itemsets.map(_.flatMap(order.get).sorted)
    //Map from integer back to item
    itemIdItemMap = order.map(_.swap)
  }

  //Add transactions to item
  if (itemsetsId != null && itemsetsId.nonEmpty) {
    //Add each of the transaction to the tree
    itemsetsId.foreach {
      itemset => {
        fptree.addTransaction(itemset, 1)
      }
    }
  }

  /**
    * Update frequency of Item
    * @param frequentMap the map from Item to its frequency
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
    * Build the order of item for the tree
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

    //sort the items in decreasing order of frequency and build the map from Item to the position in the sorted list
    this.order = items.sortWith(_.frequency > _.frequency).zipWithIndex.toMap
  }

  /**
    * Generate the conditional based patterns for one item in the header table of fpTree
    *
    * @param fpTree the tree from which conditional base patterns should be extracted
    * @param itemId the itemId for which the conditional base patterns are extracted
    * @return list of conditional base patterns
    */

  def generateConditionalBasePatterns(fpTree: FPTree, itemId: Int): FPGrowth = {

    val tmpFPGrowth = new FPGrowth(null, minCount, false)

    val headerTableItem = fptree.headerTable(itemId)

    headerTableItem.nodes.foreach(
      currentNode => {
        var pathNode = currentNode.parent

        //start inserting a conditional base transaction
        tmpFPGrowth.fptree.startInsertingTransaction()

        while (!pathNode.isRoot) {
          //Insert item to the tree
          tmpFPGrowth.fptree.addSingleItem(pathNode.itemId, currentNode.frequency)
          pathNode = pathNode.parent
        }
      }
    )

    //Return the FPGrowth object for the conditional transactions
    tmpFPGrowth
  }

  /**
    *  Generate pattern if the tree has only one single path
    *
    * @param fpTree The tree from which frequent itemsets should be extracted
    * @param inputItemId The item which should appear in all frequent itemsets
    * @return List of (itemset, frequency)
    */

  def generateFrequentFromSinglePath(fpTree: FPTree, inputItemId: Int = Int.MinValue): ListBuffer[(ListBuffer[Int], Int)] = {
    var frequentItemsets = new ListBuffer[(ListBuffer[Int], Int)]()

    //Store the path the root up to the inputItemId
    var path = new ListBuffer[FPTreeNode]

    //To store item in the path which equals to inputItemId
    var localInputItem: FPTreeNode = null

    //Init the first node
    var currentNode: FPTreeNode = null
    fpTree.root.children.foreach {
      case (item: Int, fpTreeNode: FPTreeNode) => currentNode = fpTreeNode
    }

    //Extract the path to a list
    while (currentNode != null && currentNode.frequency >= minCount && localInputItem == null) {

      if (inputItemId != Int.MinValue && currentNode.itemId.equals(inputItemId)) {
        //Update localInputItem. The path will exclude inputItemId to reduce complexity when generating all frequent itemsets
        //The inputItemId could be added later to all generated frequent itemsets
        localInputItem = currentNode
      }
      else {
        path += currentNode
      }

      //Go to next node. because the tree has single path, the first child is also the only child
      var nextNode: FPTreeNode = null
      currentNode.children.foreach {
        case (item, fpTreeNode) => nextNode = fpTreeNode
      }
      currentNode = nextNode
    }

    //bitmap indicating which item should be taken from the path
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

      //add localInputItem to the generated itemset
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
    * @return List of (ItemIdset, frequency) as frequent itemsets
    */
  def extractPattern(fpTree: FPTree, itemset: ListBuffer[Int], inputItem: Int = Int.MinValue): ListBuffer[(ListBuffer[Int], Int)] = {
    var frequentItemsets = new ListBuffer[(ListBuffer[Int], Int)]()

    if (this.fptree.hasSinglePath) {
      //If the tree has single path, generate all frequent itemsets
      val fSets = generateFrequentFromSinglePath(fpTree, inputItem)
      if (fSets.nonEmpty) {
        if (itemset != null) {
          fSets.foreach {
            //Add the itemset to the generated frequent items
            case (fItemset, support) => frequentItemsets += ((itemset ++ fItemset, support))
          }
        }
        else {
          //Add results to the global result list
          frequentItemsets ++= fSets
        }
      }
    }
    else {
      fptree.headerTable.foreach {
        case (itemId, headerTableItem) => {

          if (inputItem == Int.MinValue || itemId.equals(inputItem)) {
            //Only run when there is no inputItem or only wants to extract frequent itemsets which has inputItem
            if (headerTableItem.count >= minCount) {
              //The rest is similar fo FPGrowth method proposed in the paper
              var currentItemset = ListBuffer.empty[Int]
              currentItemset += itemId

              if (itemset != null) {
                currentItemset ++= itemset
              }
              frequentItemsets += ((currentItemset, headerTableItem.count))

              //recursively build conditional FP-Tree and mine frequent itemsets
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
    * Get all frequent itemset and map back from ItemId to Item
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
