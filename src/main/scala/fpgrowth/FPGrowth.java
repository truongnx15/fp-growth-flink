//package fpgrowth
package fpgrowth;

import java.util.List;
import java.util.Map;

/**
 * FPGrowth tree in memory
 */

public class FPGrowth {
	//Order of items in the current tree
	Map<String, Integer> order;
	private float support;
	private int numberOfItems;
	private int topK;
	
	FPGrowth(List<Itemset> itemsets) {
		buildItemOrder();
	}
	
	/**
	 * We need to build the order of item for the tree
	 */
	private void buildItemOrder() {
		//initializing order variable
	}
}
