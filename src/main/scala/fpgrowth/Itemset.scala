package fpgrowth

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Itemset[T](var items: ArrayBuffer[T], var support: Long) extends Ordered[Itemset[T]] {

  def this() = {
    this(null, 0)
    this.items = ArrayBuffer.empty[T]
    this.support = 0
  }

  /**
    * @param items the items to set
    */
  def setItems(items: ArrayBuffer[T]) {
    this.items = items
  }

  @throws(classOf[CloneNotSupportedException])
  override def clone: Itemset[T] = {
    super.clone.asInstanceOf[Itemset[T]]
  }

  def addItem(item: T) {
    items += item
  }

  override def toString: String = {
    val string: StringBuilder = new StringBuilder
    string.append("(")
    items.foreach { string.append(_)}
    string.append("):" + support)
    string.toString
  }

  override def compare(o: Itemset[T]): Int = {
    this.support compare o.support
  }
}
