package fpgrowth

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

class Itemset(var items: ListBuffer[Item], var support: Long) extends Ordered[Itemset] {

  def this() = {
    this(null, 0)
    this.items = new ListBuffer()
    this.support = 0
  }

  /**
    * @param items the items to set
    */
  def setItems(items: ListBuffer[Item]) {
    this.items = items
  }

  @throws(classOf[CloneNotSupportedException])
  override def clone: Itemset = {
    super.clone.asInstanceOf[Itemset]
  }

  def addItem(item: Item) {
    items += item
  }

  override def toString: String = {
    val string: StringBuilder = new StringBuilder
    string.append("(")
    items.foreach { string.append(_)}
    string.append("):" + support)
    string.toString
  }

  override def compare(o: Itemset): Int = {
    this.support compare o.support
  }

  def sortItems(order: immutable.Map[Item, Int]): Unit = {
    items.foreach(item => item.rank = order(item))
    items = items.sortWith( _.rank < _.rank)
  }
}
