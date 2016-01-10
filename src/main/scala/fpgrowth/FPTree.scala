//package fpgrowth
package fpgrowth

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * FPGrowth tree in memory
  */
class FPTree(var itemsets: ListBuffer[Itemset], var minCount: Long) {

  //Header table in FPGrowth
  var headerTable = mutable.HashMap.empty[Item, FPTreeNode]

  //Store last FPGrowth in the path from headerTable
  var lastHeaderTableNode = mutable.HashMap.empty[Item, FPTreeNode]

  //Root of the FPTree
  var root: FPTreeNode = new FPTreeNode(null, 0L, null)

  var hasSinglePath = true;

  /**
    * Add one transaction to the FPGrowth Tree
    * @param itemset
    */

  def addTransaction(itemset: Itemset): Unit = {
    var currentNode = root
    itemset.items.foreach {
      item => {
        val child = currentNode.children.getOrElse(item, null)
        if (child == null || !item.equals(child.item)) {
          //We should create new child be cause there is no

          val newNode = new FPTreeNode(item, 1L, currentNode )
          //Add to the children of currentNode
          currentNode.children += (item -> newNode)
          if (currentNode.children.size > 1) {
            hasSinglePath = false
          }

          if (headerTable.getOrElse(item, null) == null) {
            //Not yet in the headerTable => the first entry
            headerTable += item -> newNode
          }
          else {
            val lastItem = lastHeaderTableNode(item)
            lastItem.nextNode = newNode
          }

          //Update last node in the path from header Table
          lastHeaderTableNode += item -> newNode

          //Update current node to new node
          currentNode = newNode
        }
        else {
          //We should go down to the next node because we have common path
          currentNode = child
          currentNode.frequency += 1
        }
      }
    }
  }

  def printHeaderTable() : Unit = {
    headerTable.foreach {
      case (item, fptreeNode) => {
        var node = fptreeNode
        print(node + " (P: " + node.parent + ") ")
        do {
          node = node.nextNode
          if (node != null) {
            print( node + " (P: " + node.parent + "): ")
          }
        } while (node != null)
        println("")
      }
    }
  }

  /**
    * Print tree in Breath-First Search.
    */

  def printTree(): Unit = {
    var queue = mutable.Queue[FPTreeNode]()
    queue += root
    while (!queue.isEmpty) {
      val currentNode = queue.dequeue()
      val children = currentNode.children
      println(currentNode + ": ")
      children.foreach {
        case (item, fpTreeNode) => {
          queue += fpTreeNode
          print(fpTreeNode + " ")
        }
      }
      println("\n")
    }
  }

  /**
    * Build the FPTree
    */
  def buildFPTree(): Unit = {
    //Add each of the transaction to the tree
    itemsets.foreach {
      itemset => {
        addTransaction(itemset)
      }
    }
  }
}
