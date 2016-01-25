package fpgrowth

import scala.collection.mutable

class FPTreeNode(var itemId: Int, var frequency: Int, var parent: FPTreeNode) {

  //Store children of FPGrowth node so it could be searched quickly
  var children = mutable.HashMap.empty[Int, FPTreeNode]

  //check if the node is root node
  def isRoot: Boolean = itemId == Int.MinValue

  override def toString: String = itemId + ":" + frequency
}
