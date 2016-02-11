package fpgrowth

import scala.collection.mutable

/**
  * Node in the FP-Tree
  * @param itemId The item wrapped by the node
  * @param frequency The frequency of item
  * @param parent Parent of the node in the FP-Tree
  */

class FPTreeNode(var itemId: Int, var frequency: Int, var parent: FPTreeNode) {

  //Store children of FPGrowth node so it could be searched quickly
  var children = mutable.HashMap.empty[Int, FPTreeNode]

  //check if the node is root node
  def isRoot: Boolean = itemId == Int.MinValue

  //Just for pretty print
  override def toString: String = itemId + ":" + frequency
}
