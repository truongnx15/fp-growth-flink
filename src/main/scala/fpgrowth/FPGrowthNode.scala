package fpgrowth

import scala.collection.mutable

/**
  * Created by Xuan Truong on 10-Jan-16.
  */
class FPTreeNode(var item: Item, var frequency: Long, var parent: FPTreeNode) {

  //Store children of FPGrowth node so it could be searched quickly
  var children = mutable.HashMap.empty[Item, FPTreeNode]

  //Next to next node (used in the header table)
  var nextNode: FPTreeNode = null

  //check if the node is root node
  def isRoot = (item == null)
}
