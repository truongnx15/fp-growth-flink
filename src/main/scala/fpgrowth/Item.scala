
package fpgrowth

import org.apache.flink.core.memory.{DataOutputView, DataInputView}
import org.apache.flink.types.Key

/**
  *
  * @param name
  * @param frequency
  * @param count = 1 if the tree is built from fresh. = sum frequency of corresponding node in tree if we're building tree in conditional pattern
  */

class Item(var name: String, var frequency: Long, var count: Long) extends Serializable with Ordered[Item]{

  //Rank to sort
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
  
  override def hashCode: Int = {
    return 47 * (47 + name.length.hashCode())
  }
  
  override def compare(o: Item): Int = {
    if (this.name != o.name)
      return this.rank compare o.rank
    else
      return this.name compare o.name
  }
  
  override def toString = {
    "[" + this.name + ", " + this.frequency + ", " + this.rank + "]"
  }
}