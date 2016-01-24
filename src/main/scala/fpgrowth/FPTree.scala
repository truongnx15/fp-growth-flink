//package fpgrowth
package fpgrowth

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * FPGrowth tree in memory
  */
class FPTree(var minCount: Long) {

  //Header table in FPGrowth
  var headerTable = mutable.HashMap.empty[Item, ListBuffer[FPTreeNode]]

  //Root of the FPTree
  var root: FPTreeNode = new FPTreeNode(null, 0L, null)

  var hasSinglePath = true

  /**
    * Add one transaction to the FPGrowth Tree
    * @param itemset a transaction to be added to current FPTree
    */

  def addTransaction(itemset: Iterable[Item]): Unit = {
    var currentNode = root
    itemset.foreach {
      item => {
        val child = currentNode.children.getOrElse(item, null)
        if (child == null || !item.equals(child.item)) {
          //We should create new child be cause there is no

          val newNode = new FPTreeNode(item, item.count, currentNode )
          //Add to the children of currentNode
          currentNode.children += (item -> newNode)
          if (currentNode.children.size > 1) {
            hasSinglePath = false
          }

          headerTable.getOrElseUpdate(item, ListBuffer[FPTreeNode]()).append(newNode)

          //Update current node to new node
          currentNode = newNode
        }
        else {
          //We should go down to the next node because we have common path
          currentNode = child
          currentNode.frequency += item.count
        }

        //add frequency of item to the first element in the header table
        headerTable(item).head.item.frequency += item.count
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
