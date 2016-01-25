//package fpgrowth
package fpgrowth

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * FPGrowth tree in memory
  */
class FPTree(var minCount: Long) {

  //Header table in FPGrowth
  var headerTable = mutable.HashMap.empty[Int, ListBuffer[FPTreeNode]]
  var itemFrequencyTable = mutable.HashMap.empty[Int, Int]

  //Root of the FPTree
  var root = new FPTreeNode(Int.MinValue, 0, null)

  var hasSinglePath = true

  /**
    * Add one transaction to the FPGrowth Tree
    * @param itemset a transaction to be added to current FPTree
    */

  def addTransaction(itemset: Iterable[Int], itemsetFrequency: Int = 1): Unit = {
    var currentNode = root
    itemset.foreach {
      itemId => {
        val child = currentNode.children.getOrElse(itemId, null)
        if (child == null || !itemId.equals(child.itemId)) {
          //We should create new child be cause there is no

          val newNode = new FPTreeNode(itemId, itemsetFrequency, currentNode )
          //Add to the children of currentNode
          currentNode.children += (itemId -> newNode)
          if (currentNode.children.size > 1) {
            hasSinglePath = false
          }

          headerTable.getOrElseUpdate(itemId, ListBuffer[FPTreeNode]()).append(newNode)

          //Update current node to new node
          currentNode = newNode
        }
        else {
          //We should go down to the next node because we have common path
          currentNode = child
          currentNode.frequency += itemsetFrequency
        }

        val itemFrequency = itemFrequencyTable.getOrElseUpdate(itemId, 0) + itemsetFrequency
        itemFrequencyTable += (itemId -> itemFrequency)
      }
    }
  }

  /**
    * Print HeaderTable for the tree
    * Only for debugging
    */

  def printHeaderTable() : Unit = {
    headerTable.foreach {
      case (item, listFPTreeNode) =>
        listFPTreeNode.foreach {
          node => print(node + " (P: " + node.parent + ") ")
        }
        println()
    }
  }

  /**
    * Print tree as traversing Tree by Breath-First Search.
    * Only for debugging
    */

  def printTree(): Unit = {
    var queue = mutable.Queue[FPTreeNode]()
    queue += root
    while (queue.nonEmpty) {
      val currentNode = queue.dequeue()
      val children = currentNode.children
      println(currentNode + ": ")
      children.foreach {
        case (item, fpTreeNode) =>
          queue += fpTreeNode
          print(fpTreeNode + " ")
      }
      println("\n")
    }
  }
}
