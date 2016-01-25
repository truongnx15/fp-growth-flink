package fpgrowth

import scala.collection.mutable

/**
  * Created by Xuan Truong on 10-Jan-16.
  */
class FPTreeNode(var itemId: Int, var frequency: Int, var parent: FPTreeNode) {

  //Store children of FPGrowth node so it could be searched quickly
  var children = mutable.HashMap.empty[Int, FPTreeNode]

  //check if the node is root node
  def isRoot: Boolean = itemId == Int.MaxValue

  override def toString: String = itemId + ":" + frequency
}
