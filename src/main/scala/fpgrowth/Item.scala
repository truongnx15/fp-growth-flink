
package fpgrowth

/**
  * The wrapper for an item
  * @param name Name of the item, name should be unique for item
  * @param frequency Frequency of item
  */

class Item(var name: String, var frequency: Int) extends Serializable with Ordered[Item]{

  def this() {
    this(null, 0)
  }

  override def equals(o: Any) = o match  {
    case o: Item => this.name == o.name
    case _ => false
  }
  
  override def hashCode: Int = name.length.hashCode()

  override def compare(o: Item): Int = {
    this.name compare o.name
  }
  
  override def toString: String = {
    "[" + this.name + ", " + this.frequency + "]"
  }
}