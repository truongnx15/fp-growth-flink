//package fpgrowth
package fpgrowth

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

class Itemset(var items: ListBuffer[Item], var support: Long) extends Ordered[Itemset] {

  def this() = {
    this(null, 0)
    this.items = new ListBuffer()
    this.support = 0
  }

  /**
    * @return the items
    */
  def getItems(): ListBuffer[Item] = {
    return items
  }

  /**
    * @param items the items to set
    */
  def setItems(items: ListBuffer[Item]) {
    this.items = items
  }

  /**
    * @return the support
    */
  def getSupport(): Long = {
    return support
  }

  /**
    * @param support the support to set
    */
  def setSupport(support: Long) {
    this.support = support
  }

  @throws(classOf[CloneNotSupportedException])
  override def clone: Itemset = {
    return super.clone.asInstanceOf[Itemset]
  }

  def addItem(item: Item) {
    items += item
  }

  override def toString: String = {
    val string: StringBuilder = new StringBuilder
    string.append("(")
    items.foreach { string.append(_)}
    string.append("):" + support)
    return string.toString
  }

  override def compare(o: Itemset): Int = {
    return this.support compare o.support
  }

  def sortItems(order: immutable.Map[Item, Int], minCount: Long): Unit = {
    items.foreach(item => item.rank = order(item))
    items = items.sortWith( _ < _).filter(_.frequency >= minCount)
  }
}
