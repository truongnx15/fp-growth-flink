
package fpgrowth

/**
  *
  * @param name Name of the item, name should be unique for item
  * @param frequency Frequency of item
  * @param count = 1 if the tree is built from fresh. = sum frequency of corresponding node in tree if we're building tree in conditional pattern
  */

class Item(var name: String, var frequency: Long, var count: Long) extends Serializable with Ordered[Item]{

  //Rank to sort item
  var rank: Long = 0

  def this() {
    this(null, 0, 1)
    this.rank = frequency
  }
  
  //Constructor when only item name given
  def this(name:String) = {
    this(name, 0, 1)
    this.rank = frequency
  }

  override def equals(o: Any) = o match  {
    case o: Item => this.name == o.name
    case _ => false
  }
  
  override def hashCode: Int = 47 * (47 + name.length.hashCode())
  
  override def compare(o: Item): Int = {
    if (this.name != o.name) {
      if (this.rank != o.rank) {
        this.rank compare o.rank
      }
      this.name compare o.name
    }
    else
      this.name compare o.name
  }
  
  override def toString: String = {
    "[" + this.name + ", " + this.frequency + ", " + this.rank + "]"
  }
}