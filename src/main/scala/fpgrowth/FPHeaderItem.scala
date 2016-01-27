package fpgrowth

import scala.collection.mutable.ListBuffer

//This is inspired by summary class in Spark

class FPHeaderItem {
  var count: Int = 0
  var nodes = ListBuffer.empty[FPTreeNode]
}
