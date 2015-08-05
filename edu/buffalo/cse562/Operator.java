/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Sathish
 *
 */
public interface Operator {
	
	/**
	 * returns one tuple at a time
	 * @return
	 */
	public ArrayList<Tuple> readOneTuple();
	
	/**
	 * Returns the output tuple schema the implementer may produce
	 * @return
	 */
	public HashMap<String,ColumnDetail> getOutputTupleSchema();
	
	/**
	 * resets the iterator to the initial item
	 */
	public void reset();
	
	/**
	 * Returns its child operator
	 * @return
	 */
	public Operator getChildOp();
		
	/***
	 * Sets the child Operator 
	 */
	public void setChildOp(Operator child);
	
	/**
	 * Gets the parent Operator
	 * @param parent
	 */
	public Operator getParent();
	
	/**
	 * Sets the parent Operator
	 * @param parent
	 */
	public void setParent(Operator parent);

}
