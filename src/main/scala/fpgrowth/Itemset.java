//package fpgrowth
package fpgrowth;

import java.util.ArrayList;
import java.util.List;


public class Itemset implements Comparable<Itemset> {
	private List<Item> items;
	private int support;
	
	public Itemset() {
		items = new ArrayList<Item>();
		support = 0;
	}
	/**
	 * @return the items
	 */
	public List<Item> getItems() {
		return items;
	}
	/**
	 * @param items the items to set
	 */
	public void setItems(List<Item> items) {
		this.items = items;
	}
	/**
	 * @return the support
	 */
	public int getSupport() {
		return support;
	}
	/**
	 * @param support the support to set
	 */
	public void setSupport(int support) {
		this.support = support;
	}

	public Itemset clone() throws CloneNotSupportedException {
		return (Itemset)super.clone();
	}
	
	public void addItem(Item item) {
		items.add(item);
	}
	
	public String toString() {
		
		StringBuilder string = new StringBuilder();
		string.append("(");
		for(int i = 0; i < items.size(); i++) {
			string.append(items.get(i).toString());
		}
		string.append("):" + support);
		
		return string.toString();
	}

    public int compareTo(Itemset o) {
        return o.support - this.support;
    }
}
