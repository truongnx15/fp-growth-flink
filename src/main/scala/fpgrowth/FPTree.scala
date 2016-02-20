package fpgrowth

import scala.collection.mutable

/**
  * Local FP-Tree
  */

class FPTree {

  //Header table in FPGrowth
  var headerTable = mutable.HashMap.empty[Int, FPHeaderItem]

  //Init root of the tree
  var root = new FPTreeNode(Int.MinValue, 0, null)

  //Flag indicating if the tree has single path or not
  var hasSinglePath = true

  //temporary variable to store variable when inserting transaction to the tree
  //This variable keeps state of "the node" that has the same prefix to the transaction
  //This is to allow insert item by item to the tree i.e. insert each item to the tree without collecting all items in a transactions
  var currentInsertingNode: FPTreeNode = null

  /**
    * Insert a transaction to the tree
    * @param itemset the transaction to be inserted
    * @param itemsetFrequency the frequency of the transaction. This is to handle to case building the tree from conditional based patterns
    */

  def addTransaction(itemset: Iterable[Int], itemsetFrequency: Int = 1): Unit = {
    //Start inserting a transaction
    this.startInsertingTransaction()

    itemset.foreach {
      itemId => addSingleItem(itemId, itemsetFrequency)
    }
  }

  /**
    * Reset the currentInsertingNode to root node to start inserting new transaction
    * This allows inserting transaction item by item without collection all items in the transaction
    */
  def startInsertingTransaction(): Unit = {
    currentInsertingNode = root
  }

  /**
    * Insert an itemset to the tree
    * @param itemId the item to insert
    * @param itemFrequency the frequency of item
    */

  def addSingleItem(itemId: Int, itemFrequency: Int): Unit = {
    val headerTableItem = headerTable.getOrElseUpdate(itemId, new FPHeaderItem)
    val child = currentInsertingNode.children.getOrElse(itemId, null)
    if (child == null || !itemId.equals(child.itemId)) {
      //We should create new child be cause there is no common prefix
      val newNode = new FPTreeNode(itemId, itemFrequency, currentInsertingNode )

      //Add to the children of currentNode
      currentInsertingNode.children += (itemId -> newNode)

      //Check if the tree has a single path
      if (currentInsertingNode.children.size > 1) {
        hasSinglePath = false
      }

      //insert new node to header table
      headerTableItem.nodes += newNode

      //Update current node to new node
      currentInsertingNode = newNode
    }
    else {
      //We should go down to the next node because we have common path
      currentInsertingNode = child
      currentInsertingNode.frequency += itemFrequency
    }

    //Update frequency of item in header table
    headerTableItem.count += itemFrequency
  }

  /**
    * Print headerTable for the tree
    * Only for debugging
    */

  def printHeaderTable() : Unit = {
    headerTable.foreach {
      case (item, listFPTreeNode) =>
        listFPTreeNode.nodes.foreach {
          node => print(node + " (P: " + node.parent + ") ")
        }
        println()
    }
  }

  /**
    * Print tree by traversing Tree by Breath-First Search.
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
