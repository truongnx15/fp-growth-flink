
package fpgrowth

class Item(var name: String, var frequency: Long) extends Ordered[Item] {
  
  def this() {
    this(null, 0)
  }
  
  //Constructor when only item name given
  def this(name:String) = {
    this(name, 0)
  }
  
  override def equals(o: Any): Boolean = {
    o.isInstanceOf[Item] && this.name == o.asInstanceOf[Item].name
  }
  
  override def hashCode = {
    super.hashCode() + name.hashCode() + frequency.intValue()
  }
  
  def compare(o: Item): Int = {
    if (this.frequency != o.frequency)
      return this.frequency compare o.frequency
    else
      return this.name compare o.name
  }
  
  override def toString = {
    "[" + this.name + ", " + this.frequency + "]"
  }
}