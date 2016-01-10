//package fpgrowth
package fpgrowth

import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import util.control.Breaks._

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
        breakable {
          while (true) {
            val child = currentNode.children(item)
            if (child == null) {
              //We should create new child be cause there is no
              val newNode = new FPTreeNode(item, 0L, currentNode)

              //Add to the children of currentNode
              currentNode.children += item -> newNode
              if (currentNode.children.size > 1) {
                hasSinglePath = false
              }

              if (headerTable(item) == null) {
                //Not yet in the headerTable => the first entry
                headerTable += item -> newNode
              }
              else {
                val lastItem = lastHeaderTableNode(item)
                lastItem.nextNode = newNode
              }

              //Update last node in the path from header Table
              lastHeaderTableNode += item -> newNode

              //Stop searching
              break;
            }
            else {
              //We should go down to the next node because we have common path
              currentNode = child
              currentNode.frequency += 1
            }
          }
        }
      }
    }
  }

  /**
    * Build the FPTree
    */
  def buildFPTree(): Unit = {
    //Add each of the transaction to the tree
    itemsets.foreach {
      item => {
        addTransaction(item)
      }
    }
  }
}
