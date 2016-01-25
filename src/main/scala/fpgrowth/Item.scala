
package fpgrowth

/**
  *
  * @param name Name of the item, name should be unique for item
  * @param frequency Frequency of item
  */

class Item(var name: String, var frequency: Long) extends Serializable with Ordered[Item]{

  //Rank to sort item
  var rank: Long = 0

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
  
  override def hashCode: Int = name.length.hashCode()

  override def compare(o: Item): Int = {
    /*
    if (this.name != o.name) {
      if (this.rank != o.rank) {
        this.rank compare o.rank
      }
      this.name compare o.name
    }
    else
      this.name compare o.name
    */
    this.name compare o.name
  }
  
  override def toString: String = {
    "[" + this.name + ", " + this.frequency + ", " + this.rank + "]"
  }
}