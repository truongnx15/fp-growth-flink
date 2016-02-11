package helper


import fpgrowth.Item

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * This is a helper to read transaction from input file
  */

object IOHelper {
  /**
    * Read transactions in text file
    *
    * @param input The path to input file
    * @param itemDelimiter The delimiter of items within a transaction
    * @return List of transaction(ListBuffer[Item])
    */

  def readInput(input: String, itemDelimiter: String): List[ListBuffer[Item]] = {
    var transactions = ListBuffer[ListBuffer[Item]]()

    val tmpTransaction = Source.fromFile(input).getLines()
    tmpTransaction.foreach(
      line => {

        val itemset = ListBuffer.empty[Item]
        //Split line to get items
        val items = line.trim.split(itemDelimiter)

        if (items.nonEmpty) {
          items.foreach { x =>
            if (x.length() > 0) itemset += new Item(x, 0)
          }
          transactions += itemset
        }
      }
    )

    //Return result
    transactions.toList
  }
}
