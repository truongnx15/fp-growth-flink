package fpgrowth

import scala.collection.mutable.ListBuffer

//This is inspired by summary class in Spark

class FPHeaderItem {
  //The frequency of item
  var count: Int = 0
  //List of nodes which have similar item
  var nodes = ListBuffer.empty[FPTreeNode]
}
