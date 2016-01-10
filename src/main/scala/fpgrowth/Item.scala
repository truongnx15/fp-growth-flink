
package fpgrowth

class Item(var name: String, var frequency: Long) extends Ordered[Item] {

  //Rank to sort
  var rank: Long = this.frequency

  def this() {
    this(null, 0)
    this.rank = frequency
  }
  
  //Constructor when only item name given
  def this(name:String) = {
    this(name, 0)
    this.rank = frequency
  }
  
  override def equals(o: Any) = o match  {
    case o: Item => this.name == o.name
    case _ => false
  }
  
  override def hashCode: Int = {
    return 47 * (47 + name.length.hashCode())
  }
  
  def compare(o: Item): Int = {
    if (this.name != o.name)
      return this.rank compare o.rank
    else
      return this.name compare o.name
  }
  
  override def toString = {
    "[" + this.name + ", " + this.frequency + ", " + this.rank + "]"
  }
}